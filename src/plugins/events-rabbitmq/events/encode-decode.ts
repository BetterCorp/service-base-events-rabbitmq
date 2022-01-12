export function encode(message: Buffer | string | Object = '') {
  return Buffer.from(message.toString());
}

export function decode(msg: any) {
  return msg.content.toString();
}