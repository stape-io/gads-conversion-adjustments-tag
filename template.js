const JSON = require('JSON');
const sendHttpRequest = require('sendHttpRequest');
const getContainerVersion = require('getContainerVersion');
const logToConsole = require('logToConsole');
const getRequestHeader = require('getRequestHeader');
const encodeUriComponent = require('encodeUriComponent');
const getAllEventData = require('getAllEventData');
const makeString = require('makeString');
const makeNumber = require('makeNumber');
const getTimestampMillis = require('getTimestampMillis');
const getType = require('getType');
const sha256Sync = require('sha256Sync');
const Math = require('Math');
const Object = require('Object');
const getGoogleAuth = require('getGoogleAuth');

const isLoggingEnabled = determinateIsLoggingEnabled();
const traceId = getRequestHeader('trace-id');

const postBody = getData();
const postUrl = getUrl();
let auth;

if (data.authFlow === 'stape') {
  return sendConversionRequestApi();
} else {
  auth = getGoogleAuth({
    scopes: ['https://www.googleapis.com/auth/adwords']
  });
  return sendConversionRequest();
}

function sendConversionRequestApi() {
  if (isLoggingEnabled) {
    logToConsole(
      JSON.stringify({
        Name: 'GAdsConversionAdjustments',
        Type: 'Request',
        TraceId: traceId,
        EventName: makeString(data.conversionActionId),
        RequestMethod: 'POST',
        RequestUrl: postUrl,
        RequestBody: postBody,
      })
    );
  }

  sendHttpRequest(
    postUrl,
    (statusCode, headers, body) => {
      if (isLoggingEnabled) {
        logToConsole(
          JSON.stringify({
            Name: 'GAdsConversionAdjustments',
            Type: 'Response',
            TraceId: traceId,
            EventName: makeString(data.conversionActionId),
            ResponseStatusCode: statusCode,
            ResponseHeaders: headers,
            ResponseBody: body,
          })
        );
      };
      if (statusCode >= 200 && statusCode < 400) {
        data.gtmOnSuccess();
      } else {
        data.gtmOnFailure();
      }
    },
    {
      headers: {
        'Content-Type': 'application/json',
        'login-customer-id': data.customerId,
      }, method: 'POST'
    },
    JSON.stringify(postBody)
  );
}

function sendConversionRequest() {
  if (isLoggingEnabled) {
    logToConsole(
      JSON.stringify({
        Name: 'GAdsConversionAdjustments',
        Type: 'Request',
        TraceId: traceId,
        EventName: makeString(data.conversionActionId),
        RequestMethod: 'POST',
        RequestUrl: postUrl,
        RequestBody: postBody,
      })
    );
  }

  sendHttpRequest(
    postUrl, { headers: {'Content-Type': 'application/json', 'login-customer-id': data.customerId, 'developer-token': data.developerToken}, method: 'POST', authorization: auth}, JSON.stringify(postBody)
  ).then((statusCode, headers, body) => {
    if (isLoggingEnabled) {
      logToConsole(
        JSON.stringify({
          Name: 'GAdsConversionAdjustments',
          Type: 'Response',
          TraceId: traceId,
          EventName: makeString(data.conversionActionId),
          ResponseStatusCode: statusCode,
          ResponseHeaders: headers,
          ResponseBody: body,
        })
      );
    };
      
    if (statusCode >= 200 && statusCode < 400) {
      data.gtmOnSuccess();
    } else {
      data.gtmOnFailure();
    }
  });
}

function getUrl() {
  if (data.authFlow === 'own') {
    const apiVersion = '18';
    return (
      'https://googleads.googleapis.com/v' + apiVersion + '/customers/' +
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
  const eventData = getAllEventData();
  let mappedData = {
    conversionAction: 'customers/' + data.opCustomerId + '/conversionActions/' + data.conversionAction,
    adjustmentType: data.conversionAdjustmentType || 'UNSPECIFIED',
  };

  mappedData = addConversionAttribution(eventData, mappedData);
  mappedData = addUserIdentifiers(eventData, mappedData);

  return {
    conversionAdjustments: [mappedData],
    partialFailure: true,
    validateOnly: data.debugEnabled || false,
  };
}

function addConversionAttribution(eventData, mappedData) {
  const gclid = data.gclid || eventData.gclid;
  const conversionDateTime = data.conversionDateTime || eventData.conversionDateTime || getConversionDateTime();

  if (gclid && conversionDateTime) {
    mappedData.gclidDateTimePair = {
      gclid: gclid,
      conversionDateTime: conversionDateTime,
    };
  }

  const adjustedValue = makeNumber((data.conversionValue || eventData.conversionValue|| eventData.value || eventData['x-ga-mp1-ev'] || eventData['x-ga-mp1-tr'] || 1));
  const currencyCode = data.currencyCode || eventData.currencyCode || eventData.currency || 'USD';

  if (adjustedValue && currencyCode) {
    mappedData.restatementValue = {
      adjustedValue: adjustedValue,
      currencyCode: currencyCode,
    };
  }

  if (data.orderId) mappedData.orderId = makeString(data.orderId);
  else if (eventData.orderId) mappedData.orderId = makeString(eventData.orderId);
  else if (eventData.order_id) mappedData.orderId = makeString(eventData.order_id);
  else if (eventData.transaction_id) mappedData.orderId = makeString(eventData.transaction_id);

  if (data.adjustmentDateTime) mappedData.adjustmentDateTime = makeString(data.adjustmentDateTime);
  else if (eventData.adjustmentDateTime) mappedData.adjustmentDateTime = makeString(eventData.adjustmentDateTime);

  if (data.userAgent) mappedData.userAgent = makeString(data.userAgent);
  else if (eventData.userAgent) mappedData.userAgent = makeString(eventData.userAgent);

  return mappedData;
}

function addUserIdentifiers(eventData, mappedData) {
  let hashedEmail;
  let hashedPhoneNumber;
  let mobileId;
  let thirdPartyUserId;
  let addressInfo;
  let userIdentifiersMapped = [];
  let userEventData = {};
  let usedIdentifiers = [];


  if (getType(eventData.user_data) === 'object') {
    userEventData = eventData.user_data || eventData.user_properties || eventData.user;
  }

  if (data.userDataList) {
    let userIdentifiers = [];

    data.userDataList.forEach((d) => {
      const valueType = getType(d.value);
      const isValidValue = ['undefined', 'null'].indexOf(valueType) === -1 && d.value !== '';
      if(isValidValue) {
        let identifier = {};
        identifier[d.name] = hashData(d.name, d.value);
        identifier['userIdentifierSource'] = d.userIdentifierSource;

        userIdentifiers.push(identifier);
        usedIdentifiers.push(d.name);
      }
    });

    userIdentifiersMapped = userIdentifiers;
  }

  if (eventData.hashedEmail) hashedEmail = eventData.hashedEmail;
  else if (eventData.email) hashedEmail = eventData.email;
  else if (eventData.email_address) hashedEmail = eventData.email_address;
  else if (userEventData.email) hashedEmail = userEventData.email;
  else if (userEventData.email_address) hashedEmail = userEventData.email_address;

  if (usedIdentifiers.indexOf('hashedEmail') === -1 && hashedEmail) {
    userIdentifiersMapped.push({
      hashedEmail: hashData('hashedEmail', hashedEmail),
      userIdentifierSource: 'UNSPECIFIED',
    });
  }

  if (eventData.phone) hashedPhoneNumber = eventData.phone;
  else if (eventData.phone_number) hashedPhoneNumber = eventData.phone_number;
  else if (userEventData.phone) hashedPhoneNumber = userEventData.phone;
  else if (userEventData.phone_number) hashedPhoneNumber = userEventData.phone_number;

  if (
    usedIdentifiers.indexOf('hashedPhoneNumber') === -1 &&
    hashedPhoneNumber
  ) {
    userIdentifiersMapped.push({
      hashedPhoneNumber: hashData('hashedPhoneNumber', hashedPhoneNumber),
      userIdentifierSource: 'UNSPECIFIED',
    });
  }

  if (eventData.mobileId) mobileId = eventData.mobileId;

  if (usedIdentifiers.indexOf('mobileId') === -1 && mobileId) {
    userIdentifiersMapped.push({
      mobileId: mobileId,
      userIdentifierSource: 'UNSPECIFIED',
    });
  }

  if (eventData.thirdPartyUserId) thirdPartyUserId = eventData.thirdPartyUserId;

  if (usedIdentifiers.indexOf('thirdPartyUserId') === -1 && thirdPartyUserId) {
    userIdentifiersMapped.push({
      thirdPartyUserId: thirdPartyUserId,
      userIdentifierSource: 'UNSPECIFIED',
    });
  }

  if (eventData.addressInfo) addressInfo = eventData.addressInfo;

  if (usedIdentifiers.indexOf('addressInfo') === -1 && addressInfo) {
    userIdentifiersMapped.push({
      addressInfo: addressInfo,
      userIdentifierSource: 'UNSPECIFIED',
    });
  }

  if (userIdentifiersMapped.length) {
    mappedData.userIdentifiers = userIdentifiersMapped;
  }

  return mappedData;
}

function getConversionDateTime() {
  return convertTimestampToISO(getTimestampMillis());
}

function isHashed(value) {
  if (!value) {
    return false;
  }

  return makeString(value).match('^[A-Fa-f0-9]{64}$') !== null;
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

  if(type === 'object') {
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
    value = value
      .split(' ')
      .join('')
      .split('-')
      .join('')
      .split('(')
      .join('')
      .split(')')
      .join('');
  } else if (key === 'hashedEmail') {
    let valueParts = value.split('@');

    if (valueParts[1] === 'gmail.com' || valueParts[1] === 'googlemail.com') {
      value = valueParts[0].split('.').join('') + '@' + valueParts[1];
    } else {
      value = valueParts.join('@');
    }
  }

  return sha256Sync(value, { outputEncoding: 'hex' });
}

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
    let isLeapYear = !(year % 4);
    let nextTimestamp = timestamp - daysToMs(isLeapYear ? 366 : 365);
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
    let msInThisMonth = daysToMs(daysByMonth[i]);
    if (timestamp > msInThisMonth) {
      timestamp = timestamp - msInThisMonth;
    } else {
      month = i + 1;
      break;
    }
  }
  let date = Math.ceil(timestamp / daysToMs(1));
  timestamp = timestamp - daysToMs(date - 1);
  let hours = Math.floor(timestamp / hoursToMs(1));
  timestamp = timestamp - hoursToMs(hours);
  let minutes = Math.floor(timestamp / minToMs(1));
  timestamp = timestamp - minToMs(minutes);
  let sec = Math.floor(timestamp / secToMs(1));

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

function determinateIsLoggingEnabled() {
  const containerVersion = getContainerVersion();
  const isDebug = !!(
    containerVersion &&
    (containerVersion.debugMode || containerVersion.previewMode)
  );

  if (!data.logType) {
    return isDebug;
  }

  if (data.logType === 'no') {
    return false;
  }

  if (data.logType === 'debug') {
    return isDebug;
  }

  return data.logType === 'always';
}

function enc(data) {
  data = data || '';
  return encodeUriComponent(data);
}
