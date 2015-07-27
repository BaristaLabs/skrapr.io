var Consumer = require('sqs-consumer');
var AWS = require("aws-sdk");
var url = require("url");
var moment = require("moment");
var got = require("got");
var isUrl = require("is-url");
var Q = require("q");
var readAllStream = require('read-all-stream');

var s3 = new AWS.S3();
var sns = new AWS.SNS({ apiVersion: '2010-03-31' });

var crawlContent = function (html) {
    
    //TODO: Simply add the url to the main skrapr crawl queue.

    //var params = {
    //    Message: imgSrc,
    //    TopicArn: "arn:aws:sns:us-east-1:672288119474:skrapr_io_fleet_newUrl"
    //};
    
    //sns.publish(params, function (err, data) {
    //    if (err) {
    //        console.log(err, err.stack);
    //    }
    //    else {
    //        console.log(moment().format("MM/DD hh:mm:ss") + ": Submitted url to skrape " + imgSrc);
    //    }
    //});
};

var getUrlKey = function (targetUrl) {
    targetUrl = url.parse(targetUrl);
    return targetUrl.host + targetUrl.pathname;
};

var checkIfUrlHasBeenPreviouslyRetrieved = function (targetUrl) {
    return Q.promise(function (resolve, reject, notify) {
        
        var targetUrlKey = getUrlKey(targetUrl);

        //Test if the file already exists.
        var checkParams = { Bucket: process.env.dockyardBucket, Key: targetUrlKey };
        s3.headObject(checkParams, function (err, data) {
            if (err) {
                if (err.statusCode === 403) //Hmm. this should be 404, but we're getting a forbidden.
                    resolve(false);
                else
                    reject(err);
            } else {
                //We got a successful response. we're done.
                resolve(true)
            }
        });
    });
};

var downloadContentAtUrl = function (targetUrl) {
    return Q.promise(function (resolve, reject, notify) {
        var gotParams = {
            method: "GET",
            encoding: null,
            headers: {
                "pragma": "no-cache",
                "cache-control": "no-cache",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36",
                "Accept-Encoding": "gzip, deflate, sdch",
                "Accept-Language": "en-US,en; q = 0.8"
            }
        };
        
        var finalUrl = targetUrl;
        var finalResponse = null;
        var stream = got.stream(targetUrl, gotParams)
            .on('redirect', function (response, nextOptions) {
                console.log(moment().format("MM/DD hh:mm:ss") + ": Download Redirected to " + nextOptions.href);
                finalUrl = nextOptions.href;
            })
            .on('response', function (response) {
                console.log(moment().format("MM/DD hh:mm:ss") + ": Downloaded " + finalUrl + " (" + response.headers['content-type'] + ") " + response.headers['content-length']);
                response.initialUrl = targetUrl,
                response.finalUrl = finalUrl;
                finalResponse = response;
            })
            .on('error', function (error, body, response){
                console.log(moment().format("MM/DD hh:mm:ss") + ": Error retrieving " + targetUrl);
                reject(err);
            });

            readAllStream(stream, null, function (err, data) {
                if (err) {
                    reject(err);
                    return;
                }

                finalResponse.body = data;
                resolve(finalResponse)
            });
    });
};

var uploadContentToDockyard = function (response, message) {
    return Q.promise(function (resolve, reject, notify) {
        var finalUrlKey = getUrlKey(response.finalUrl);

        var uploadParams = {
            Bucket: process.env.dockyardBucket,
            Key: finalUrlKey,
            Body: response.body,
            ContentType: response.headers['content-type'],
            Metadata: {
                originMessageId: message.MessageId,
                originUrl: response.initialUrl,
                finalUrl: response.finalUrl,
                retrievedOn: Date.now().toString()
            }
        };

        //Upload our content to s3.
        var uploadOptions = { partSize: 10 * 1024 * 1024, queueSize: 1 };
        s3.upload(uploadParams, uploadOptions, function (err, data) {
            if (err) {
                console.log(moment().format("MM/DD hh:mm:ss") + ": Error storing " + response.finalUrl);
                reject(err);
            }
            else {
                console.log(moment().format("MM/DD hh:mm:ss") + ": Stored " + response.finalUrl + " as " + data.Location);
                resolve(response);
            }
        });
    });
};

var uploadRedirectorObjectToDockyard = function (response, message) {
    return Q.promise(function (resolve, reject, notify) {
        var initialUrlKey = getUrlKey(response.initialUrl);
        var finalUrlKey = getUrlKey(response.finalUrl);
        
        var putParams = {
            Bucket: process.env.dockyardBucket,
            Key: initialUrlKey,
            Body: "",
            ContentType: response.headers['content-type'],
            Metadata: {
                originMessageId: message.MessageId,
                originUrl: response.initialUrl,
                finalUrl: response.finalUrl,
                retrievedOn: Date.now().toString()
            },
            WebsiteRedirectLocation: "/" + finalUrlKey
        };
        
        s3.putObject(putParams, function (err, data) {
            if (err) {
                console.log(moment().format("MM/DD hh:mm:ss") + ": Error storing redirection" + initialUrlKey);
                reject(err);
            }
            else {
                console.log(moment().format("MM/DD hh:mm:ss") + ": Stored " + initialUrlKey + " as redirection to " + finalUrlKey);
                resolve(response);
            }
        });
    });
};

var app = Consumer.create({
    queueUrl: process.env.dockyardQueueUrl,
    handleMessage: function (message, done) {
        console.log("=======================================");
        console.log(moment().format("MM/DD hh:mm:ss") + ": Received Message " + message.MessageId);
        
        var headers = [];
        var messageBody = JSON.parse(message.Body);
        var targetUrl = messageBody.Message;

        checkIfUrlHasBeenPreviouslyRetrieved(targetUrl)
            .then(function (hasBeenRetrieved) {
                if (hasBeenRetrieved) {
                    //TODO: Improve this so we can force a re-get.
                    console.log(moment().format("MM/DD hh:mm:ss") + ": Skipping " + targetUrl + " as it was previously retrieved.");
                    return null;
                } else {
                    console.log(moment().format("MM/DD hh:mm:ss") + ": Downloading " + targetUrl);
                    return downloadContentAtUrl(targetUrl);
                }
            })
            .then(function (downloadResponse) {
                if (downloadResponse == null)
                    return null;
            
                return uploadContentToDockyard(downloadResponse, message);
            })
            .then(function (downloadResponse) {
                if (downloadResponse == null)
                    return null;

                //If the final url and the initial url don't match, place a 0-byte redirector object that contains metadata about the target.
                if (downloadResponse.initialUrl !== downloadResponse.finalUrl)
                    return uploadRedirectorObjectToDockyard(downloadResponse, message)
                else
                    return downloadResponse;
            })
            .then(function (downloadResponse) {
                done();
            })
            .catch(function (err) {
                done(err);
            })
            .finally(function () {
                console.log("=======================================");
            })
            .done();
    }
});

app.on('error', function (err) {
    console.log("Error:")
    console.log("\t", err.message);
});

console.log("Listening...");
app.start();