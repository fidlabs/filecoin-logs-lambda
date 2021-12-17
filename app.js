const zlib = require('zlib');
var fs = require("fs")
const AWS = require('aws-sdk');
var uuid = require('uuid');

let dynamodb = new AWS.DynamoDB({ apiVersion: '2012-10-17' });

exports.handler = async (event, context, cb) => {
    console.log("Event: " + JSON.stringify(event, null, 2))
    try {
        switch (event.type) {
            case "POST_CUSTOM_LOGS":
                resp = postCustomLogs(event, context)
                break
            case "GET_LOGS":
                resp = await getLogsBy(event, context)
                break
            default:
                resp = postAwlogs(event, context)
                break
        }
        return cb(null, resp)
    } catch (error) {
        console.error(error)
        return cb(null, 500)
    }



}

// event.type = POST_CUSTOM_LOGS
// event.logArray : { (from request)
//  message,
//  type,
//  actionKeyword;
//  repo 
//  issueNumber
// }[]
// to create here:
//  ID,
//  dateTimestamp : Date
const postCustomLogs = async (event, context) => {
    const logArray = event.logArray
    let objArray = []
    for (let i = 0; i < logArray.length; i++) {
        let obj = {
            "PutRequest": {
                "Item": {
                    "ID": {
                        "S": uuid.v1().toString()
                    },
                    "dateTimestamp": {
                        "S": new Date().toString()
                    },
                    "message": {
                        "S": logArray[i].message
                    },
                    "type": {
                        "S": logArray[i].type
                    },
                    "actionKeyword": {
                        "S": logArray[i].actionKeyword
                    },
                    "repo": {
                        "S": logArray[i].repo
                    },
                    "issue_number": {
                        "N": logArray[i].issueNumber
                    }
                }
            }
        }
        objArray.push(obj)
    }

    const batchItem = {
        "RequestItems": {
            "dle-crud": objArray
        }
    }

    try {
        return await saveManyRecursively(batchItem)
    } catch (error) {
        console.log(error)
    }

}

// event.searchType: ID || dateTimestamp  || issue_number NUMBERS
// event.search: string || number (the thing to retrieve)
// event.operation: = || < || >
const getLogsBy = async (event, context) => {

    const params = {
        TableName: "dle-crud",
        IndexName: `${event.searchType}-index`,
        Select: "ALL_PROJECTED_ATTRIBUTES",
        ExpressionAttributeValues: {
            ":v1": {
                N: event.search
            }
        },
        KeyConditionExpression: `${event.searchType} = :v1`
    };

    try {
        return await getLogsByRecursively(params, context)
    } catch (error) {
        console.log(error)
    }
}

const getLogsByRecursively = async (params, context) => {
    try {
        const dynamoProm = dynamodb.query(params).promise()
        const dynamoRes = await Promise.resolve(dynamoProm)
        if (dynamoRes.LastEvaluatedKey) {
            params.LastEvaluatedKey = dynamoRes.LastEvaluatedKey //TODO TEST
            getLogsByRecursively(params, context)
        }

        const response = {
            statusCode: 200,
            items: dynamoRes.Items
        }

        return response
    } catch (error) {
        console.log("error in error in getLogsByRecursively", error)
    }
}

function postAwlogs(event, context) {
    var payload = new Buffer.from(event.awslogs.data, 'base64')
    return zlib.gunzip(payload, function (e, decodedEvent) {
        if (e) {
            context.fail(e)
        } else {
            const decEvent = JSON.parse(decodedEvent.toString('ascii'))
            let logArray = decEvent.logEvents

            console.log("logArray", logArray.length)
            console.log("decodedEvent", decEvent)
            //build the objects to save 

            let objArray = []
            for (let i = 0; i < logArray.length; i++) {
                const regexIssueNumber = /(n\s*|#\s*|number\s*)([0-9]+)/gi
                const issueNumber = logArray[i].message.match(regexIssueNumber) !== null ? logArray[i].message.match(regexIssueNumber)[0].replace(/number|n|#|/gi, "").trim() : false
                if (!issueNumber) continue
                const date = new Date(logArray[i].timestamp).toString()
                let obj = {
                    "PutRequest": {
                        "Item": {
                            "ID": {
                                "S": uuid.v1().toString()
                            },
                            "dateTimestamp": {
                                "S": date
                            },
                            "message": {
                                "S": logArray[i].message
                            },
                            "type": {
                                "S": returnTypeOfLog(logArray[i].message) // debug, warn, ecc
                            },
                            "actionKeyword": {
                                "S": returnActionKeyword(logArray[i].message) // commented ....
                            },
                            "repo": {
                                "S": returnRepo(logArray[i].message)
                            },
                            "issue_number": {
                                "N": issueNumber
                            }
                        }
                    }
                }
                objArray.push(obj)
            }

            const batchItem = {
                "RequestItems": {
                    "dle-crud": objArray
                }
            }

            return saveManyRecursively(batchItem)

        }
    })
}

function returnRepo(message) {
    const regex = /\[(.*?)\]/i
    const exec = regex.exec(message)
    if (exec != null) {
        return exec[0]
    }
    return ""
}
function returnTypeOfLog(message) {
    const regex = /(INFO|WARN|DEBUG|ERROR)/ig
    const exec = regex.exec(message)
    if (exec != null) {
        return exec[0]
    }
    return "INFO"
}
function returnActionKeyword(message) {
    const regex = /(commented|skipped|margin|multisig created|issue created|datacap modified|Posting label|CREATE REQUEST COMMENT|CREATE STATS COMMENT)/ig
    const exec = regex.exec(message)
    if (exec != null) {
        return exec[0]
    }
    return ""
}
const saveManyRecursively = async (batchItem) => {

    try {
        const dynamoProm = dynamodb.batchWriteItem(batchItem).promise()
        const dynamoRes = await Promise.resolve(dynamoProm)

        if (Object.keys(dynamoRes.UnprocessedItems).length > 0) {
            setTimeout(() => console.log("waiting 2 secs before retry...", 2000))
            saveManyRecursively(res.UnprocessedItems) //TODO TEST
        }

        const response = {
            statusCode: 200,
        }

        return response
    } catch (error) {
        console.log("error in error in saveManyRecursively", error)
    }

}




