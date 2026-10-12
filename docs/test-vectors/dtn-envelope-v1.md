# DTN Envelope v1 — Deterministic Wire Vector

**Status:** draft fixture for the proposed schema; no production key material.  
**Purpose:** test CBOR/COSE encoding and signature verification independently of transport, time, or application execution.

The signing key is the publicly documented RFC 8032 Ed25519 test seed. It MUST NOT be reused outside tests. The fixture trust resolver maps the 32-byte `kid` below to the listed public key and to the listed opaque `sender_id`; that mapping is test setup, not a production key-ID derivation algorithm.

## Expected fields

- Version: `1`
- Message ID: `000102030405060708090a0b0c0d0e0f`
- Sender ID: bytes `20` through `3f` (32 bytes)
- Created at: `1800000000000` ms
- Expires at: `1800003600000` ms
- Traffic class: `1` (durable)
- Payload type: `text/plain`
- Payload UTF-8 bytes: `hello DTN`
- SHA-256(payload): `d8da449081fffb5c5b73288d2217eab50053bcd32a0f7395dfbdcdb34b06b435`
- Protected COSE algorithm: `-8` (EdDSA; fixture public key is Ed25519)
- Protected content type: `application/vnd.luminous.dtn-envelope-v1+cbor`
- `kid`: `00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff`
- Ed25519 public key (raw): `d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a`
- Signature length: 64 bytes
- Full tagged COSE_Sign1 object length: 295 bytes

These fixed times are fixture data only. Tests of expiry must inject a deterministic clock; the fixture's signature/encoding verification must not depend on the current wall clock.

## Expected deterministic CBOR payload

```text
a901010250000102030405060708090a0b0c0d0e0f035820202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f041b000001a3185c5000051b000001a318933e800601076a746578742f706c61696e084968656c6c6f2044544e095820d8da449081fffb5c5b73288d2217eab50053bcd32a0f7395dfbdcdb34b06b435
```

## Expected complete tagged COSE_Sign1 bytes

Concatenate the hex lines without whitespace. The top-level object begins with CBOR tag 18 (`d2`); the unprotected header is the empty CBOR map (`a0`), not a byte string. The protected header is itself deterministic CBOR encoded inside a byte string. The signature is Ed25519 over the RFC 9052 COSE `Sig_structure`:

```text
d2845856a3012703782d6170706c69636174696f6e2f766e642e6c756d696e6f
75732e64746e2d656e76656c6f70652d76312b63626f72045820001122334455
66778899aabbccddeeff00112233445566778899aabbccddeeffa05888a90101
0250000102030405060708090a0b0c0d0e0f035820202122232425262728292a
2b2c2d2e2f303132333435363738393a3b3c3d3e3f041b000001a3185c500005
1b000001a318933e800601076a746578742f706c61696e084968656c6c6f2044
544e095820d8da449081fffb5c5b73288d2217eab50053bcd32a0f7395dfbdcd
b34b06b4355840433a7e379ebdf4ee3f03f65743a08eb211a96f2438f2d1eca8
102df9cd7d5f0ef1b2cb11179efb7fbac192f05f9f7baced7a272fac03e82e6a
de8bf2df476307
```

## Required negative vectors derived from this fixture

A future implementation must generate these as automated tests, not just document them:

1. Flip one bit in the payload bytes without changing the signature: signature verification fails.
2. Flip one bit in the signature: signature verification fails.
3. Change the protected `kid` without recomputing the signature: signature verification fails before the credential-binding result can be accepted.
4. Re-sign a payload whose `payload_sha256` is wrong: signature may verify, but envelope validation fails on digest mismatch.
5. Supply a valid signature under a trusted key whose resolved principal differs from `sender_id`: reject as a binding mismatch.
6. Replay the same signed bytes for the same sender/message ID: return the persisted receipt; dispatch count stays unchanged.
7. Re-sign different signed content with the same sender/message ID: reject as an ID conflict; dispatch count stays unchanged.
8. Use this fixture with an injected clock after `expires_at_ms`: reject as expired even after process restart.
9. Encode equivalent CBOR non-deterministically, add an unknown map key, duplicate a map key, or append trailing bytes: reject.
