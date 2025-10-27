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
const BigQuery = require('BigQuery');

/**********************************************************************************************/

const traceId = getRequestHeader('trace-id');
const apiVersion = '22';
const eventData = getAllEventData();

if (!isConsentGivenOrNotRequired()) {
  return data.gtmOnSuccess();
}

const url = eventData.page_location || getRequestHeader('referer');
if (url && url.lastIndexOf('https://gtm-msr.appspot.com/', 0) === 0) {
  return data.gtmOnSuccess();
}

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

/**********************************************************************************************/
// Vendor related functions

function sendConversionRequestApi() {
  log({
    Name: 'GAdsConversionAdjustments',
    Type: 'Request',
    TraceId: traceId,
    EventName: makeString(data.conversionAction),
    RequestMethod: 'POST',
    RequestUrl: postUrl,
    RequestBody: postBody
  });

  sendHttpRequest(
    postUrl,
    (statusCode, headers, body) => {
      log({
        Name: 'GAdsConversionAdjustments',
        Type: 'Response',
        TraceId: traceId,
        EventName: makeString(data.conversionAction),
        ResponseStatusCode: statusCode,
        ResponseHeaders: headers,
        ResponseBody: body
      });

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
        'x-gads-api-version': apiVersion
      },
      method: 'POST'
    },
    JSON.stringify(postBody)
  );
}

function sendConversionRequest() {
  log({
    Name: 'GAdsConversionAdjustments',
    Type: 'Request',
    TraceId: traceId,
    EventName: makeString(data.conversionAction),
    RequestMethod: 'POST',
    RequestUrl: postUrl,
    RequestBody: postBody
  });

  sendHttpRequest(
    postUrl,
    {
      headers: {
        'Content-Type': 'application/json',
        'login-customer-id': data.customerId,
        'developer-token': data.developerToken
      },
      method: 'POST',
      authorization: auth
    },
    JSON.stringify(postBody)
  ).then((result) => {
    // .then has to be used when the Authorization header is in use
    log({
      Name: 'GAdsConversionAdjustments',
      Type: 'Response',
      TraceId: traceId,
      EventName: makeString(data.conversionAction),
      ResponseStatusCode: result.statusCode,
      ResponseHeaders: result.headers,
      ResponseBody: result.body
    });

    if (result.statusCode >= 200 && result.statusCode < 400) {
      data.gtmOnSuccess();
    } else {
      data.gtmOnFailure();
    }
  });
}

function getUrl() {
  if (data.authFlow === 'own') {
    return (
      'https://googleads.googleapis.com/v' +
      apiVersion +
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
    conversionAction: 'customers/' + data.opCustomerId + '/conversionActions/' + data.conversionAction,
    adjustmentType: data.conversionAdjustmentType || 'UNSPECIFIED'
  };

  mappedData = addConversionAttribution(eventData, mappedData);
  mappedData = addUserIdentifiers(eventData, mappedData);

  return {
    conversionAdjustments: [mappedData],
    partialFailure: true,
  };
}

function addConversionAttribution(eventData, mappedData) {
  const gclid = data.gclid || eventData.gclid;
  const conversionDateTime = data.conversionDateTime || eventData.conversionDateTime;

  if (gclid && conversionDateTime) {
    mappedData.gclidDateTimePair = {
      gclid: gclid,
      conversionDateTime: conversionDateTime
    };
  }

  const adjustedValue = makeNumber(
    data.conversionValue ||
      eventData.conversionValue ||
      eventData.value ||
      eventData['x-ga-mp1-ev'] ||
      eventData['x-ga-mp1-tr'] ||
      1
  );
  const currencyCode = data.currencyCode || eventData.currencyCode || eventData.currency || 'USD';

  if (adjustedValue && currencyCode && data.conversionAdjustmentType !== 'RETRACTION') {
    mappedData.restatementValue = {
      adjustedValue: adjustedValue,
      currencyCode: currencyCode
    };
  }

  if (data.orderId) mappedData.orderId = makeString(data.orderId);
  else if (eventData.orderId) mappedData.orderId = makeString(eventData.orderId);
  else if (eventData.order_id) mappedData.orderId = makeString(eventData.order_id);
  else if (eventData.transaction_id) mappedData.orderId = makeString(eventData.transaction_id);

  if (data.adjustmentDateTime) mappedData.adjustmentDateTime = makeString(data.adjustmentDateTime);
  else if (eventData.adjustmentDateTime) mappedData.adjustmentDateTime = makeString(eventData.adjustmentDateTime);
  else mappedData.adjustmentDateTime = getConversionDateTime();

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
  const usedIdentifiers = [];

  if (getType(eventData.user_data) === 'object') {
    userEventData = eventData.user_data || eventData.user_properties || eventData.user;
  }

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

  if (eventData.hashedEmail) hashedEmail = eventData.hashedEmail;
  else if (eventData.email) hashedEmail = eventData.email;
  else if (eventData.email_address) hashedEmail = eventData.email_address;
  else if (userEventData.email) hashedEmail = userEventData.email;
  else if (userEventData.email_address) hashedEmail = userEventData.email_address;

  if (usedIdentifiers.indexOf('hashedEmail') === -1 && hashedEmail) {
    userIdentifiersMapped.push({
      hashedEmail: hashData('hashedEmail', hashedEmail),
      userIdentifierSource: 'UNSPECIFIED'
    });
  }

  if (eventData.phone) hashedPhoneNumber = eventData.phone;
  else if (eventData.phone_number) hashedPhoneNumber = eventData.phone_number;
  else if (userEventData.phone) hashedPhoneNumber = userEventData.phone;
  else if (userEventData.phone_number) hashedPhoneNumber = userEventData.phone_number;

  if (usedIdentifiers.indexOf('hashedPhoneNumber') === -1 && hashedPhoneNumber) {
    userIdentifiersMapped.push({
      hashedPhoneNumber: hashData('hashedPhoneNumber', hashedPhoneNumber),
      userIdentifierSource: 'UNSPECIFIED'
    });
  }

  if (eventData.mobileId) mobileId = eventData.mobileId;

  if (usedIdentifiers.indexOf('mobileId') === -1 && mobileId) {
    userIdentifiersMapped.push({
      mobileId: mobileId,
      userIdentifierSource: 'UNSPECIFIED'
    });
  }

  if (eventData.thirdPartyUserId) thirdPartyUserId = eventData.thirdPartyUserId;

  if (usedIdentifiers.indexOf('thirdPartyUserId') === -1 && thirdPartyUserId) {
    userIdentifiersMapped.push({
      thirdPartyUserId: thirdPartyUserId,
      userIdentifierSource: 'UNSPECIFIED'
    });
  }

  if (eventData.addressInfo) addressInfo = eventData.addressInfo;

  if (usedIdentifiers.indexOf('addressInfo') === -1 && addressInfo) {
    userIdentifiersMapped.push({
      addressInfo: addressInfo,
      userIdentifierSource: 'UNSPECIFIED'
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

/**********************************************************************************************/
// Helpers

function isHashed(value) {
  if (!value) return false;
  return makeString(value).match('^[A-Fa-f0-9]{64}$') !== null;
}

function enc(data) {
  data = data || '';
  return encodeUriComponent(data);
}

function isConsentGivenOrNotRequired() {
  if (data.adStorageConsent !== 'required') return true;
  if (eventData.consent_state) return !!eventData.consent_state.ad_storage;
  const xGaGcs = eventData['x-ga-gcs'] || ''; // x-ga-gcs is a string like "G110"
  return xGaGcs[2] === '1';
}

function log(rawDataToLog) {
  const logDestinationsHandlers = {};
  if (determinateIsLoggingEnabled()) logDestinationsHandlers.console = logConsole;
  if (determinateIsLoggingEnabledForBigQuery()) logDestinationsHandlers.bigQuery = logToBigQuery;

  const keyMappings = {
    // No transformation for Console is needed.
    bigQuery: {
      Name: 'tag_name',
      Type: 'type',
      TraceId: 'trace_id',
      EventName: 'event_name',
      RequestMethod: 'request_method',
      RequestUrl: 'request_url',
      RequestBody: 'request_body',
      ResponseStatusCode: 'response_status_code',
      ResponseHeaders: 'response_headers',
      ResponseBody: 'response_body'
    }
  };

  for (const logDestination in logDestinationsHandlers) {
    const handler = logDestinationsHandlers[logDestination];
    if (!handler) continue;

    const mapping = keyMappings[logDestination];
    const dataToLog = mapping ? {} : rawDataToLog;

    if (mapping) {
      for (const key in rawDataToLog) {
        const mappedKey = mapping[key] || key;
        dataToLog[mappedKey] = rawDataToLog[key];
      }
    }

    handler(dataToLog);
  }
}

function logConsole(dataToLog) {
  logToConsole(JSON.stringify(dataToLog));
}

function logToBigQuery(dataToLog) {
  const connectionInfo = {
    projectId: data.logBigQueryProjectId,
    datasetId: data.logBigQueryDatasetId,
    tableId: data.logBigQueryTableId
  };

  dataToLog.timestamp = getTimestampMillis();

  ['request_body', 'response_headers', 'response_body'].forEach((p) => {
    dataToLog[p] = JSON.stringify(dataToLog[p]);
  });

  const bigquery = getType(BigQuery) === 'function' ? BigQuery() /* Only during Unit Tests */ : BigQuery;
  bigquery.insert(connectionInfo, [dataToLog], { ignoreUnknownValues: true });
}

function determinateIsLoggingEnabled() {
  const containerVersion = getContainerVersion();
  const isDebug = !!(containerVersion && (containerVersion.debugMode || containerVersion.previewMode));

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

function determinateIsLoggingEnabledForBigQuery() {
  if (data.bigQueryLogType === 'no') return false;
  return data.bigQueryLogType === 'always';
}