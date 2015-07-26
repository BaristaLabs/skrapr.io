var Download = require("download");
var Consumer = require('sqs-consumer');

var app = Consumer.create({
    queueUrl: process.env.dockWorkerQueueUrl,
    handleMessage: function (message, done) {
        console.log(new Date() + ": Received Message " + message.MessageId);
        
        var headers = null;
        var messageBody = JSON.parse(message.Body);

        new Download()
            .get(messageBody.Message)
            .use(function (response, url) {
                headers = response.headers;
            })
            .run(function (err, files) {
            
            if (err) {
                console.log(new Date() + ": Error retrieving " + messageBody.Message);
                console.log(err);
                done(err);
            }
            else {
                console.log(new Date() + ": Downloaded " + files.length + " files");
                files.forEach(function (file) {
                    console.log("\t" + file.url + "\t" + headers['content-type']);
                })
                done();
            }
        });
    }
});

app.on('error', function (err) {
    console.log(err.message);
});

console.log("Listening...");
app.start();