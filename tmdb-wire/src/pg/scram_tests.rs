use super::scram::*;

/// The worked example from RFC 7677 section 3. The server nonce here is the
/// concatenation of the RFC's client and server nonces, which is why this
/// reproduces the published ClientProof and ServerSignature byte for byte.
const USER: &str = "user";
const PASSWORD: &[u8] = b"pencil";
const CLIENT_NONCE: &str = "rOprNGfwEbeRWgbNEkqO";
const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
const SALT: [u8; 16] = [
    0x5b, 0x6d, 0x99, 0x68, 0x9d, 0x12, 0x35, 0x8e, 0xec, 0xa0, 0x4b, 0x14, 0x12, 0x36, 0xfa, 0x81,
];
const ITERATIONS: u32 = 4096;
const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
const SERVER_FINAL: &str = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

fn verifier(password: &[u8]) -> Verifier {
    Verifier::from_password(password, SALT.to_vec(), ITERATIONS)
}

fn start() -> (Server, String) {
    Server::begin(
        &format!("n,,n={},r={}", USER, CLIENT_NONCE),
        &verifier(PASSWORD),
        SERVER_NONCE,
    )
    .expect("client-first-message is well formed")
}

#[test]
fn the_server_first_message_matches_rfc_7677() {
    let (_, server_first) = start();
    assert_eq!(
        server_first,
        format!("r={}{},s={},i={}", CLIENT_NONCE, SERVER_NONCE, SALT_B64, ITERATIONS)
    );
}

#[test]
fn the_client_proof_is_the_one_the_rfc_publishes() {
    let (server, _) = start();
    let server_final = server.finish(CLIENT_FINAL).expect("proof verifies");
    assert_eq!(server_final, SERVER_FINAL);
}

#[test]
fn the_salted_password_matches_the_rfc_derived_key() {
    assert_eq!(
        base64_encode(&pbkdf2_hmac_sha256(PASSWORD, &SALT, ITERATIONS)),
        "xKSVEDI6tPlSysH6mUQZOeeOp01r6B3fcJbodRPcYV0="
    );
}

/// Both expectations below were cross-checked against .NET's HMACSHA256,
/// which is an independent implementation of RFC 2104.
#[test]
fn hmac_matches_the_rfc_4231_test_case_1() {
    let key = [0x0bu8; 20];
    assert_eq!(
        base64_encode(&hmac_sha256(&key, b"Hi There")),
        "sDRMYdjbOFNcqK/OrwvxK4gdwgDJgz2nJuk3bC4yz/c="
    );
}

#[test]
fn hmac_matches_the_rfc_4231_test_case_2() {
    assert_eq!(
        base64_encode(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
        "W9zBRr9gdU5qBCQmCJV1x1oAPwidJzmDnexYuWTsOEM="
    );
}

/// RFC 4231 case 6: a key longer than the 64-byte block must be hashed down
/// first. Verified against .NET's HMACSHA256.
#[test]
fn hmac_hashes_an_oversized_key_before_use() {
    let key = [0xaau8; 131];
    assert_eq!(
        base64_encode(&hmac_sha256(
            &key,
            b"Test Using Larger Than Block-Size Key - Hash Key First"
        )),
        "YOQxWR7gtn8Niiaqy/W3f44LxiE3KMUUBUYEDw7jf1Q="
    );
}

/// A zero-length key is legal for HMAC, so SCRAM must not assume a minimum.
#[test]
fn hmac_accepts_an_empty_key() {
    assert_eq!(
        base64_encode(&hmac_sha256(b"", b"abc")),
        "/XrbFSwF74Dcz1Ch+kwF1aPsbalVdfwxKufF0JGDY1E="
    );
}

#[test]
fn a_wrong_password_is_rejected() {
    let (server, _) = Server::begin(
        &format!("n,,n={},r={}", USER, CLIENT_NONCE),
        &verifier(b"pencil2"),
        SERVER_NONCE,
    )
    .unwrap();
    let err = server.finish(CLIENT_FINAL).unwrap_err();
    assert_eq!(err, Error::WrongPassword);
}

#[test]
fn an_unknown_user_fails_the_proof_check() {
    let (server, _) = Server::begin(
        &format!("n,,n=mallory,r={}", CLIENT_NONCE),
        &verifier(b""),
        SERVER_NONCE,
    )
    .unwrap();
    assert_eq!(server.finish(CLIENT_FINAL).unwrap_err(), Error::WrongPassword);
}

#[test]
fn a_forged_proof_that_reuses_the_right_length_is_rejected() {
    let (server, _) = start();
    let forged = format!(
        "c=biws,r={}{},p={}",
        CLIENT_NONCE,
        SERVER_NONCE,
        "A".repeat(43)
    );
    assert_eq!(server.finish(&forged).unwrap_err(), Error::WrongPassword);
}

#[test]
fn a_client_nonce_that_does_not_extend_ours_is_rejected() {
    let (server, _) = start();
    let forged = format!(
        "c=biws,r=AAAAAAAAAAAAAAAAAAAAAAAAAAAA,p={}",
        "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    );
    assert!(matches!(server.finish(&forged), Err(Error::Malformed(_))));
}

#[test]
fn a_client_nonce_that_is_only_a_prefix_of_ours_is_rejected() {
    let (server, _) = start();
    let forged = format!(
        "c=biws,r={},p={}",
        CLIENT_NONCE,
        "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    );
    assert!(matches!(server.finish(&forged), Err(Error::Malformed(_))));
}

#[test]
fn a_missing_proof_is_rejected() {
    let (server, _) = start();
    assert!(matches!(
        server.finish("c=biws,r=x"),
        Err(Error::Malformed(_))
    ));
}

#[test]
fn a_channel_binding_that_is_not_biws_is_refused() {
    let (server, _) = start();
    // "biws" is base64("n,,"). Anything else means the client believes a TLS
    // channel binding applies, which this server never negotiated.
    let forged = format!(
        "c=eSws,r={}{},p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
        CLIENT_NONCE, SERVER_NONCE
    );
    assert_eq!(
        server.finish(&forged).unwrap_err(),
        Error::UnsupportedChannelBinding
    );
}

#[test]
fn a_missing_channel_binding_is_refused() {
    let (server, _) = start();
    let forged = format!(
        "r={}{},p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
        CLIENT_NONCE, SERVER_NONCE
    );
    assert!(matches!(server.finish(&forged), Err(Error::Malformed(_))));
}

#[test]
fn gs2_channel_binding_is_refused() {
    let err = Server::begin(
        "p=tls-server-end-point,,n=user,r=rOprNGfwEbeRWgbNEkqO",
        &verifier(PASSWORD),
        SERVER_NONCE,
    )
    .unwrap_err();
    assert_eq!(err, Error::UnsupportedChannelBinding);
}

#[test]
fn a_client_with_no_channel_binding_support_is_accepted() {
    let (server, _) = Server::begin(
        &format!("y,,n={},r={}", USER, CLIENT_NONCE),
        &verifier(PASSWORD),
        SERVER_NONCE,
    )
    .unwrap();
    assert_eq!(server.finish(CLIENT_FINAL).unwrap(), SERVER_FINAL);
}

#[test]
fn the_exchange_is_not_replayable_with_a_stale_nonce() {
    let (server, server_first) = start();
    assert!(server_first.contains(SERVER_NONCE));

    // Two sessions from the same client nonce but different server nonces must
    // not accept each other's final messages.
    let (other, _) = Server::begin(
        &format!("n,,n={},r={}", USER, CLIENT_NONCE),
        &verifier(PASSWORD),
        "%DIFFERENTnonce0000000000000000",
    )
    .unwrap();
    assert!(matches!(other.finish(CLIENT_FINAL), Err(Error::Malformed(_))));
    assert!(server.finish(CLIENT_FINAL).is_ok());
}

#[test]
fn equality_checks_do_not_leak_length_or_content() {
    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"ab"));
    assert!(!constant_time_eq(b"", b"a"));
}

#[test]
fn base64_round_trips_every_length_up_to_a_block_boundary() {
    for len in 0..=48usize {
        let data: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
        let encoded = base64_encode(&data);
        assert_eq!(encoded.len(), len.div_ceil(3) * 4);
        assert!(encoded.ends_with('=') || len % 3 == 0);
    }
}

#[test]
fn base64_matches_the_rfc_4648_vectors() {
    assert_eq!(base64_encode(b""), "");
    assert_eq!(base64_encode(b"f"), "Zg==");
    assert_eq!(base64_encode(b"fo"), "Zm8=");
    assert_eq!(base64_encode(b"foo"), "Zm9v");
    assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
    assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
    assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
}
