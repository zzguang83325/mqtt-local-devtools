"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const zlib = require("node:zlib");
const {
  decodeMany,
  describePayload,
  decompressPacket,
  detectCompression,
  MAX_DECOMPRESSED_BYTES
} = require("../mqtt-decoder.js");

function utf8(value) {
  const bytes = Buffer.from(value, "utf8");
  return Buffer.concat([Buffer.from([bytes.length >> 8, bytes.length & 0xff]), bytes]);
}

function publishFrame(topic, payload) {
  const body = Buffer.concat([utf8(topic), payload]);
  assert.ok(body.length < 128, "test helper only supports short packets");
  return Buffer.concat([Buffer.from([0x30, body.length]), body]);
}

test("detectCompression recognizes gzip and zlib headers only", () => {
  assert.equal(detectCompression(Uint8Array.from([0x1f, 0x8b, 0x08, 0x00])).format, "gzip");
  assert.equal(detectCompression(Uint8Array.from([0x78, 0x9c, 0x01])).format, "zlib");
  assert.equal(detectCompression(Uint8Array.from([0x78, 0xda, 0x01])).format, "zlib");
  assert.equal(detectCompression(Uint8Array.from([0x78, 0x01, 0x01])).format, "zlib");
  assert.equal(detectCompression(Uint8Array.from([0x7b, 0x22, 0x61, 0x22])), null);
  assert.equal(detectCompression(Uint8Array.from([0x1f, 0x8b])), null);
  assert.equal(detectCompression(new Uint8Array(0)), null);
});

test("describePayload keeps raw bytes only for compressed payloads", () => {
  const gz = new Uint8Array(zlib.gzipSync(Buffer.from('{"a":1}')));
  const compressed = describePayload(gz);
  assert.equal(compressed.payloadEncoding, "binary");
  assert.equal(compressed.payloadText, undefined);
  assert.equal(compressed.payloadBytes.length, gz.length);
  assert.match(compressed.payloadHex, /^1f 8b 08/);

  const plain = describePayload(new Uint8Array(Buffer.from('{"a":1}')));
  assert.equal(plain.payloadEncoding, "utf-8");
  assert.equal(plain.payloadText, '{"a":1}');
  assert.equal(plain.payloadBytes, undefined);

  const otherBinary = describePayload(Uint8Array.from([0x00, 0xff, 0xfe, 0x01]));
  assert.equal(otherBinary.payloadEncoding, "binary");
  assert.equal(otherBinary.payloadBytes, undefined);
});

test("decompressPacket expands gzip JSON without mutating the original packet", async () => {
  const raw = JSON.stringify({ sn: "5200", status: "online", battery: 88, text: "中文内容" });
  const gz = new Uint8Array(zlib.gzipSync(Buffer.from(raw, "utf8")));
  const packet = describePayload(gz);

  const enriched = await decompressPacket(packet);

  assert.equal(enriched.compression.format, "gzip");
  assert.equal(enriched.compression.compressedSize, gz.length);
  assert.equal(enriched.compression.decompressedSize, Buffer.byteLength(raw, "utf8"));
  assert.equal(enriched.decompressed.text, raw);
  assert.equal(enriched.decompressed.encoding, "utf-8");
  assert.equal(enriched.decompressed.truncated, false);
  assert.deepEqual(JSON.parse(enriched.decompressed.text), JSON.parse(raw));
  // Original capture is untouched and still carries the compressed bytes.
  assert.equal(packet.decompressed, undefined);
  assert.equal(packet.payloadBytes.length, gz.length);
  assert.match(enriched.payloadHex, /^1f 8b 08/);
});

test("decompressPacket expands zlib payloads", async () => {
  const raw = "topic=status&value=42";
  const deflated = new Uint8Array(zlib.deflateSync(Buffer.from(raw, "utf8")));
  const enriched = await decompressPacket(describePayload(deflated));

  assert.equal(enriched.compression.format, "zlib");
  assert.equal(enriched.decompressed.text, raw);
});

test("decompressPacket keeps binary decompressed output as hex", async () => {
  const binary = Buffer.from([0x00, 0x01, 0xff, 0xfe, 0x10]);
  const gz = new Uint8Array(zlib.gzipSync(binary));
  const enriched = await decompressPacket(describePayload(gz));

  assert.equal(enriched.decompressed.encoding, "binary");
  assert.equal(enriched.decompressed.text, undefined);
  assert.equal(enriched.decompressed.hex, "00 01 ff fe 10");
  assert.equal(enriched.decompressed.size, binary.length);
});

test("decompressPacket reports corrupt gzip data instead of throwing", async () => {
  const corrupt = Uint8Array.from([0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xde, 0xad]);
  const enriched = await decompressPacket(describePayload(corrupt));

  assert.equal(enriched.compression.format, "gzip");
  assert.ok(enriched.compression.error, "corrupt data must surface an error");
  assert.equal(enriched.decompressed, undefined);
});

test("decompressPacket passes through uncompressed packets", async () => {
  const packet = describePayload(new Uint8Array(Buffer.from('{"plain":true}')));
  assert.equal(await decompressPacket(packet), packet);
});

test("decompressed output is bounded to avoid decompression bombs", async () => {
  const huge = Buffer.alloc(MAX_DECOMPRESSED_BYTES + 1024, 0x61);
  const gz = new Uint8Array(zlib.gzipSync(huge));
  const enriched = await decompressPacket(describePayload(gz));

  assert.equal(enriched.compression.truncated, true);
  assert.match(enriched.compression.error, /4 MiB/);
  assert.equal(enriched.decompressed, undefined);
});

test("decodeMany marks a compressed PUBLISH payload for later decompression", async () => {
  const raw = '{"sn":"5200","status":"online"}';
  const gz = zlib.gzipSync(Buffer.from(raw, "utf8"));
  const result = decodeMany(publishFrame("demo/data/device/5200/status", gz), 4);

  assert.equal(result.packets.length, 1);
  const packet = result.packets[0];
  assert.equal(packet.typeName, "PUBLISH");
  assert.equal(packet.topic, "demo/data/device/5200/status");
  assert.equal(packet.payloadEncoding, "binary");
  assert.equal(packet.payloadText, undefined);

  const enriched = await decompressPacket(packet);
  assert.equal(enriched.decompressed.text, raw);
  assert.equal(enriched.topic, packet.topic);
});
