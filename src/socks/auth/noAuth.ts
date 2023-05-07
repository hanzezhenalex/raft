const NO_AUTH_RESP = new Uint8Array([0x05, 0x00]);

export var noAuth = {
  method: 0x05,
  methodReplyCache: NO_AUTH_RESP,
};
