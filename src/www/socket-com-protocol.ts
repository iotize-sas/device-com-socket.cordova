import { bufferToHexString } from '@iotize/common/byte-converter';
import { ComProtocolConnectOptions, ConnectionState } from '@iotize/device-client.js/protocol/api';
import { QueueComProtocol } from '@iotize/device-client.js/protocol/core';
import { BehaviorSubject, Observable, Observer, of, Subject } from 'rxjs';
import { first, share } from 'rxjs/operators';

import { debug } from './debug';
import { SocketInterface, SocketState, SocketStatic } from './socket';
import { sleep } from './util';

declare var Socket: SocketStatic;

export interface SocketProtocolOptions {
    host: string;
    port: string | number;
}

export class CordovaSocketProtocol extends QueueComProtocol {

    private _socket: SocketInterface;
    private _receiveStream: Subject<Uint8Array>;
    private _socketOptions: SocketProtocolOptions;
    private _socketState = new BehaviorSubject(SocketState.CLOSED);

    constructor(options: SocketProtocolOptions) {
        super();
        this.options.send.timeout = 10000;
        this.options.connect.timeout = 10000;
        this._socketOptions = options;
    }

    write(data: Uint8Array): Promise<any> {
        return new Promise<Uint8Array>((resolve, reject) => {
            try {
                // if (!this.socket || !this.socket.writable){
                //     throw new Error("Socket is not open or not writable");
                // }
                debug('SocketProtocol', "write()", data);
                this._socket.write(data, () => {
                    debug("SocketProtocol", "write", "DONE");
                    resolve();
                }, (err) => {
                    debug("SocketProtocol", "write error", err);
                    reject(err);
                });
            }
            catch (err) {
                reject(err);
            }
        });
    }

    read(): Promise<Uint8Array> {
        return this.readOneValue();
    }

    readOneValue(): Promise<Uint8Array> {
        debug("SocketProtocol", "readOneValue", "INIT");
        return this._receiveStream.pipe(first()).toPromise();
        // return new Promise((resolve, reject) => {
        //     this._receiveStream.pipe(first()).subscribe((data: Uint8Array) => {
        //         debug("SocketProtocol", "readOneValue", "RESOLVING...", bufferToHexString(data));
        //         resolve(data);
        //     },
        //         (error) => {
        //             debug("SocketProtocol", "readOneValue", "REJECTED");
        //             reject(error);
        //         });
        // })
    }

    _connect(options?: ComProtocolConnectOptions): Observable<any> {
        debug('sokcet connection', this._socketOptions);
        this._socket = this._createSocket();
        this._receiveStream = new Subject<Uint8Array>();

        return Observable
            .create((emitter: Observer<any>) => {
                try {
                    if (this._socket.state == SocketState.OPENED) {
                        emitter.next(this._socket);
                        emitter.complete();
                        return;
                    }
                    if (!this._socketOptions.port || !this._socketOptions.port) {
                        // TODO proper error
                        throw new Error(`Invalid socket info. Missing port or host`);
                    }
                    this._socket.open(
                        this._socketOptions.host,
                        this._socketOptions.port,
                        () => {
                            this._socketState.next(this._socket.state);
                            emitter.next(this._socket);
                            emitter.complete();
                        },
                        (err) => {
                            this._socketState.next(this._socket.state);
                            emitter.error(typeof err === "string" ? new Error(err) : err);
                        }
                    );
                }
                catch (err) {
                    emitter.error(err);
                }
            })
            .pipe(
                // concat(this._waitForSocketConnectionState(SocketState.OPENED)),
                share()
            );
    }

    _waitForSocketConnectionState(state: SocketState): Observable<SocketState> {
        debug('_waitForSocketConnectionState', state, 'current', Socket.State[this._socket.state]);
        if (this._socket.state === state) {
            return of(this._socket.state);
        }
        else {
            return Observable.create(async (emitter: Observer<any>) => {
                while (this._socket.state != state) {
                    await sleep(100);
                }
                this._socketState.next(this._socket.state);
                emitter.next(this._socket.state);
                emitter.complete();
            });
        }
    }

    _disconnect(options?: ComProtocolConnectOptions): Observable<any> {
        return Observable.create((observer: Observer<any>) => {
            this._socket.close(
                (val) => {
                    debug('[SocketProtocol] closed', val ? val : '');
                    observer.next(val);
                    observer.complete();
                },
                (err) => {
                    debug('[SocketProtocol]', err);
                    observer.error(typeof err === "string" ? new Error(err) : err);
                });
        })
            .pipe(
                share()
            );
    }

    protected _createSocket() {
        const socket: SocketInterface = new Socket();
        socket.onData = (data: Uint8Array) => {
            debug("SocketProtocol", "onData", data);
            this.notifyNewMessage(data);
        };

        socket.onClose = (data) => {
            debug("SocketProtocol", "socket closed: ", data);
            this.setConnectionState(ConnectionState.DISCONNECTED);
            this._socketState.next(SocketState.CLOSED);
        };

        socket.onError = (error) => {
            debug("SocketProcol", "socket error: ", error);
            if (socket.state == Socket.State.OPENED) {
                this.setConnectionState(ConnectionState.DISCONNECTED);
            }
            this._socketState.next(socket.state);
            // TODO notify event
            // TODO handle disconnection event
        };
        return socket;
    }

    notifyNewMessage(message: Uint8Array) {
        // if (!this._receiveStream){
        //     throw new Error("IllegalState no receive stream");
        // }
        debug(`notifyNewMessage() ${bufferToHexString(message)}`);
        this._receiveStream.next(message);
    }

    receiveStream(): Observable<Uint8Array> {
        return this._receiveStream;
    }
}