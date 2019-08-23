export interface SocketInterface {
    state: SocketState;
    onData: (data: any) => void;
    onClose: (data: any) => void;
    onError: (data: any) => void;

    write(data: any, success: () => void, error: (err: Error) => void): void;
    close(success: (val: any) => void, error: (err: Error) => void): void;
    open(host: string, port: string | number, success: () => void, error: (err: Error) => void): void;
}

export enum SocketState {
    CLOSED,
    OPENING,
    OPENED,
    CLOSING
}

export type SocketStatic = any;