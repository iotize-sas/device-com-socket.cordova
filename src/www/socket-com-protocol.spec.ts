import { CordovaSocketProtocol } from "./socket-com-protocol";

describe("CordovaSocketProtocol", function () {

    it("create instance should work", function(){
        const instance = new CordovaSocketProtocol({
            host: 'localhost',
            port: 2000
        });
    });

});