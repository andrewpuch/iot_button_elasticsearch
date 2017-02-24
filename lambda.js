var elasticsearch = require("elasticsearch"),
    moment        = require("moment"),
    promise       = require("bluebird"),
    config        = require("./config.json");

var client = new elasticsearch.Client({ host : config.host, log : 'error' });

var createDocument = function(event) {
    return new promise(function(resolve, reject) {
        client.index({
          index: config.index + moment().utc().format('YYYY-MM-DD'),
          type: config.type,
          body: {
                serialNumber : event.serialNumber,
                batteryVoltage : parseInt(event.batteryVoltage.replace(/mV/g, "")),
                clickType : event.clickType,
                clickDate : moment().utc().format('YYYY-MM-DDTHH:mm:ss.SSSZ')
            }
        }, function(error, response, status) {
            if(error !== undefined) {
                reject(error);
            } else {
                resolve(status);
            }
        });
    });
};

var createIndex = function() {
    return new promise(function(resolve, reject) {
        client.indices.create({
            index : config.index + moment().utc().format('YYYY-MM-DD'),
            mappings : {
                click : { 
                    properties : {
                        serialNumber : { 
                            type : "string" 
                        },
                        batteryVoltage : { 
                            type : "integer" 
                        },
                        clickType : { 
                            type : "string" 
                        },
                        clickDate : {
                            type : "date", 
                            format : "date_time" 
                        }
                    }
                }
            }
        }, function(error, response, status) {
            if(error !== undefined) {
                reject(error);
            } else {
                resolve(status);
            }
        });
    });
};

var checkIndexExists = function() {
    return new promise(function(resolve, reject) {
        client.indices.exists({
            index: config.index + moment().utc().format('YYYY-MM-DD')
        }, function(error, response, status) {
            resolve(response);
        });
    });
};

exports.iotPush = function(event, context, callback) {
    checkIndexExists().then(function(exists) {
        if(exists !== true) {
            return createIndex();
        }

        return;
    }).then(function() {
        return createDocument(event);
    }).then(function(status) {
        context.done(null, { status : status });
    });    
};