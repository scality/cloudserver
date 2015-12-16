function algoCheck(signatureLength) {
    let algo;
    // If the signature sent is 44 characters,
    // this means that sha256 was used:
    // 44 characters in base64
    const SHA256LEN = 44;
    const SHA1LEN = 28;
    if (signatureLength === SHA256LEN) {
        algo = 'sha256';
    }
    if (signatureLength === SHA1LEN) {
        algo = 'sha1';
    }
    return algo;
}

export default algoCheck;
