import http from "http"
import io from "socket.io"
import metadata from "../metadata/wrapper.js"
const _ = require("underscore")

let iosock;
let queries = [];

function intersection(a, b) {
    return _.intersection(a, b);
}

function union(a, b) {
    return _.union(a, b);
}

function difference(u, a) {
    return _.difference(u, a);
}

function proccessLogicalOps(terms) {
    let i;
    while (terms.length > 1) {
        for (i = terms.length - 1; i >= 0; i--) {
            if (terms[i] === "op/AND"
                || terms[i] === "op/OR"
                || terms[i] === "op/NOT") {
                    break;
            }
        }
        if (terms[i] === "op/AND") {
            const op1 = terms[i + 1];
            const op2 = terms[i + 2];
            terms.splice(i, 3, intersection(op1, op2));
        } else if (terms[i] === "op/OR") {
            const op1 = terms[i + 1];
            const op2 = terms[i + 2];
            terms.splice(i, 3, union(op1, op2));
        }
    }
    return terms[0];
}

export default {
    createPublisher(port, log) {
        const server = http.createServer(function(req, res){
        });
        server.listen(port);
        iosock = io.listen(server);
        iosock.sockets.on("connection", function(socket) {
            log.info("indexing server connected")
            socket.on("disconnect", function() {
            });
            socket.on("subscribe", function(room) {
                socket.join(room);
            });
            socket.on("query_response", function(msg) {
                for (let i = 0; i < queries.length; i++) {
                    if (queries[i].id === msg.id) {
                        queries[i].responses -=1
                        let term_index = queries[i].query.indexOf(msg.term);
                        queries[i].query[term_index] = msg.result;
                        if (queries[i].responses === 0) {
                            let result = proccessLogicalOps(queries[i].query);
                            let listingParams = queries[i].listingParams;
                            queries.splice(i, 1)
                            metadata.respondQueryGetMD(result, listingParams);
                            break;
                        }
                    }
                }
            });
        });
    },

    putObjectMD(bucketName, objName, objVal) {
        let msg = {
            bucketName,
            objName,
            objVal
        };
        if (iosock.sockets.adapter.rooms["put"]) {
            iosock.sockets.to("put").emit("put", msg);
        }
    },

    listObject(bucketName, listingParams, cb) {
        const id = Math.random().toString(36).substring(7);
        let pending_query = {
            id,
            query: listingParams.query,
            responses : 0,
            listingParams
        }
        pending_query.listingParams.bucketName = bucketName;
        pending_query.listingParams.cb = cb;
        listingParams.query.forEach(term => {
            if (term.indexOf("op/") === -1) {
                pending_query.responses += 1
                iosock.sockets.to(term.split("/")[0]).emit("query", {
                    id,
                    term,
                    bucketName
                });
            }
        });
        queries.push(pending_query);
    },

    deleteObjectMD(bucketName, objName) {
        iosock.sockets.to("deletes").emit("delete", {
            bucketName,
            objName
        });
    },

    processQueryHeader(header) {
        if (!header) {
            return header;
        }
        const queryTerms = header.split("&");
        const query = [];
        for (let i = 0; i < queryTerms.length; i++) {
            query.push(queryTerms[i]);
        }
        return query;
    }
}
