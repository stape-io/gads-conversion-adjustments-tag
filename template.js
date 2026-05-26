const JSON = require('JSON');
const Math = require('Math');
const Object = require('Object');
const encodeUriComponent = require('encodeUriComponent');
const getAllEventData = require('getAllEventData');
const getGoogleAuth = require('getGoogleAuth');
const getRequestHeader = require('getRequestHeader');
const getTimestampMillis = require('getTimestampMillis');
const getType = require('getType');
const makeNumber = require('makeNumber');
const makeString = require('makeString');
const sendHttpRequest = require('sendHttpRequest');
const sha256Sync = require('sha256Sync');

/*==============================================================================
==============================================================================*/

const API_VERSION = '24';
const eventData = getAllEventData();

if (!isConsentGivenOrNotRequired(data, eventData)) {
  return data.gtmOnSuccess();
}

const url = eventData.page_location || getRequestHeader('referer');
if (url && url.lastIndexOf('https://gtm-msr.appspot.com/', 0) === 0) {
  return data.gtmOnSuccess();
}

return sendConversionRequest();

/*==============================================================================
  Vendor related functions
==============================================================================*/

function sendConversionRequest() {
  const postUrl = getUrl();
  const postBody = getData();
  const options = {
    headers: {
      'Content-Type': 'application/json',
      'login-customer-id': data.customerId
    },
    method: 'POST'
  };

  if (data.authFlow === 'own') {
    const auth = getGoogleAuth({
      scopes: ['https://www.googleapis.com/auth/adwords']
    });
    options.authorization = auth;
    options.headers['developer-token'] = data.developerToken;
  } else {
    options.headers['x-gads-api-version'] = API_VERSION;
  }

  sendHttpRequest(postUrl, options, JSON.stringify(postBody))
    .then((result) => {
      // .then has to be used when the Authorization header is in use
      const parsedBody = JSON.parse(result.body || '{}');

      if (result.statusCode >= 200 && result.statusCode < 400 && !parsedBody.partialFailureError) {
        return data.gtmOnSuccess();
      }
      return data.gtmOnFailure();
    })
    .catch((result) => {
      return data.gtmOnFailure();
    });
}

function getUrl() {
  if (data.authFlow === 'own') {
    return (
      'https://googleads.googleapis.com/v' +
      API_VERSION +
      '/customers/' +
      enc(data.opCustomerId) +
      ':uploadConversionAdjustments'
    );
  }

  const containerIdentifier = getRequestHeader('x-gtm-identifier');
  const defaultDomain = getRequestHeader('x-gtm-default-domain');
  const containerApiKey = getRequestHeader('x-gtm-api-key');
  return (
    'https://' +
    enc(containerIdentifier) +
    '.' +
    enc(defaultDomain) +
    '/stape-api/' +
    enc(containerApiKey) +
    '/v2/gads/auth-proxy/adjustments'
  );
}

function getData() {
  let mappedData = {
    conversionAction:
      'customers/' + data.opCustomerId + '/conversionActions/' + data.conversionAction,
    adjustmentType: data.conversionAdjustmentType || 'UNSPECIFIED'
  };

  mappedData = addConversionAttribution(eventData, mappedData);
  if (data.conversionAdjustmentType === 'ENHANCEMENT') {
    mappedData = addUserIdentifiers(eventData, mappedData);
  }

  return {
    conversionAdjustments: [mappedData],
    partialFailure: true,
    validateOnly: data.debugEnabled || false
  };
}

function addConversionAttribution(eventData, mappedData) {
  const autoMapEnabled = data.hasOwnProperty('autoMapConversionInformation')
    ? data.autoMapConversionInformation
    : true; // To accomodate a breaking change.

  const gclid = data.gclid || (autoMapEnabled ? eventData.gclid : undefined);
  const conversionDateTime =
    data.conversionDateTime || (autoMapEnabled ? eventData.conversionDateTime : undefined);

  if (gclid && conversionDateTime) {
    mappedData.gclidDateTimePair = {
      gclid: gclid,
      conversionDateTime: conversionDateTime
    };
  }

  const adjustedValue = makeNumber(
    data.conversionValue ||
      (autoMapEnabled
        ? eventData.conversionValue ||
          eventData.value ||
          eventData['x-ga-mp1-ev'] ||
          eventData['x-ga-mp1-tr']
        : undefined) ||
      1
  );
  const currencyCode =
    data.currencyCode ||
    (autoMapEnabled ? eventData.currencyCode || eventData.currency : undefined) ||
    'USD';

  if (adjustedValue && currencyCode && data.conversionAdjustmentType !== 'RETRACTION') {
    mappedData.restatementValue = {
      adjustedValue: adjustedValue,
      currencyCode: currencyCode
    };
  }

  const orderId =
    data.orderId ||
    (autoMapEnabled
      ? eventData.orderId || eventData.order_id || eventData.transaction_id
      : undefined);
  if (orderId) mappedData.orderId = makeString(orderId);

  const adjustmentDateTime =
    data.adjustmentDateTime ||
    (autoMapEnabled ? eventData.adjustmentDateTime : undefined) ||
    getConversionDateTime();
  if (adjustmentDateTime) mappedData.adjustmentDateTime = makeString(adjustmentDateTime);

  if (data.conversionAdjustmentType === 'ENHANCEMENT') {
    const userAgent = data.userAgent || (autoMapEnabled ? eventData.userAgent : undefined);
    if (userAgent) mappedData.userAgent = makeString(userAgent);
  }

  return mappedData;
}

function addUserIdentifiers(eventData, mappedData) {
  const autoMapEnabled = data.hasOwnProperty('autoMapUserData') ? data.autoMapUserData : true; // To accomodate a breaking change.

  let hashedEmail;
  let hashedPhoneNumber;
  let addressInfo;
  let userIdentifiersMapped = [];
  let userEventData = {};
  const usedIdentifiers = [];

  if (data.userDataList) {
    const userIdentifiers = [];

    data.userDataList.forEach((d) => {
      const valueType = getType(d.value);
      const isValidValue = ['undefined', 'null'].indexOf(valueType) === -1 && d.value !== '';
      if (isValidValue) {
        const identifier = {};
        identifier[d.name] = hashData(d.name, d.value);
        identifier['userIdentifierSource'] = d.userIdentifierSource;

        userIdentifiers.push(identifier);
        usedIdentifiers.push(d.name);
      }
    });

    userIdentifiersMapped = userIdentifiers;
  }

  if (autoMapEnabled) {
    if (getType(eventData.user_data) === 'object') {
      userEventData = eventData.user_data || eventData.user_properties || eventData.user;
    }

    hashedEmail =
      eventData.hashedEmail ||
      eventData.email ||
      eventData.email_address ||
      userEventData.email ||
      userEventData.email_address;
    if (usedIdentifiers.indexOf('hashedEmail') === -1 && hashedEmail) {
      userIdentifiersMapped.push({
        hashedEmail: hashData('hashedEmail', hashedEmail),
        userIdentifierSource: 'UNSPECIFIED'
      });
    }

    hashedPhoneNumber =
      eventData.phone ||
      eventData.phone_number ||
      userEventData.phone ||
      userEventData.phone_number;
    if (usedIdentifiers.indexOf('hashedPhoneNumber') === -1 && hashedPhoneNumber) {
      userIdentifiersMapped.push({
        hashedPhoneNumber: hashData('hashedPhoneNumber', hashedPhoneNumber),
        userIdentifierSource: 'UNSPECIFIED'
      });
    }

    addressInfo = eventData.addressInfo;
    if (usedIdentifiers.indexOf('addressInfo') === -1 && addressInfo) {
      userIdentifiersMapped.push({
        addressInfo: addressInfo,
        userIdentifierSource: 'UNSPECIFIED'
      });
    }
  }

  if (userIdentifiersMapped.length) {
    mappedData.userIdentifiers = userIdentifiersMapped;
  }

  return mappedData;
}

function getConversionDateTime() {
  return convertTimestampToISO(getTimestampMillis());
}

function hashData(key, value) {
  if (!value) {
    return value;
  }

  const type = getType(value);

  if (type === 'undefined' || value === 'undefined') {
    return undefined;
  }

  if (type === 'array') {
    return value.map((val) => {
      return hashData(key, val);
    });
  }

  if (type === 'object') {
    return Object.keys(value).reduce((acc, val) => {
      acc[val] = hashData(key, value[val]);
      return acc;
    }, {});
  }

  if (isHashed(value)) {
    return value;
  }

  value = makeString(value).trim().toLowerCase();

  if (key === 'hashedPhoneNumber') {
    value = value.split(' ').join('').split('-').join('').split('(').join('').split(')').join('');
  } else if (key === 'hashedEmail') {
    const valueParts = value.split('@');

    if (valueParts[1] === 'gmail.com' || valueParts[1] === 'googlemail.com') {
      value = valueParts[0].split('.').join('') + '@' + valueParts[1];
    } else {
      value = valueParts.join('@');
    }
  }

  return sha256Sync(value, { outputEncoding: 'hex' });
}

/*==============================================================================
  Helpers
==============================================================================*/

function convertTimestampToISO(timestamp) {
  const secToMs = function (s) {
    return s * 1000;
  };
  const minToMs = function (m) {
    return m * secToMs(60);
  };
  const hoursToMs = function (h) {
    return h * minToMs(60);
  };
  const daysToMs = function (d) {
    return d * hoursToMs(24);
  };
  const format = function (value) {
    return value >= 10 ? value.toString() : '0' + value;
  };
  const fourYearsInMs = daysToMs(365 * 4 + 1);
  let year = 1970 + Math.floor(timestamp / fourYearsInMs) * 4;
  timestamp = timestamp % fourYearsInMs;

  while (true) {
    const isLeapYear = !(year % 4);
    const nextTimestamp = timestamp - daysToMs(isLeapYear ? 366 : 365);
    if (nextTimestamp < 0) {
      break;
    }
    timestamp = nextTimestamp;
    year = year + 1;
  }

  const daysByMonth =
    year % 4 === 0
      ? [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
      : [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  let month = 0;
  for (let i = 0; i < daysByMonth.length; i++) {
    const msInThisMonth = daysToMs(daysByMonth[i]);
    if (timestamp > msInThisMonth) {
      timestamp = timestamp - msInThisMonth;
    } else {
      month = i + 1;
      break;
    }
  }
  const date = Math.ceil(timestamp / daysToMs(1));
  timestamp = timestamp - daysToMs(date - 1);
  const hours = Math.floor(timestamp / hoursToMs(1));
  timestamp = timestamp - hoursToMs(hours);
  const minutes = Math.floor(timestamp / minToMs(1));
  timestamp = timestamp - minToMs(minutes);
  const sec = Math.floor(timestamp / secToMs(1));

  return (
    year +
    '-' +
    format(month) +
    '-' +
    format(date) +
    ' ' +
    format(hours) +
    ':' +
    format(minutes) +
    ':' +
    format(sec) +
    '+00:00'
  );
}

function isHashed(value) {
  if (!value) return false;
  return makeString(value).match('^[A-Fa-f0-9]{64}$') !== null;
}

function enc(data) {
  if (['null', 'undefined'].indexOf(getType(data)) !== -1) data = '';
  return encodeUriComponent(makeString(data));
}

function isConsentGivenOrNotRequired(data, eventData) {
  if (data.adStorageConsent !== 'required') return true;
  if (eventData.consent_state) return !!eventData.consent_state.ad_storage;
  const xGaGcs = eventData['x-ga-gcs'] || ''; // x-ga-gcs is a string like "G110"
  return xGaGcs[2] === '1';
}
