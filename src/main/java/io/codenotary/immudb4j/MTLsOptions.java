package io.codenotary.immudb4j;

public class MTLsOptions {

    public String serverName;
    public String pKey;
    public String certificate;
    public String clientCAs;

    public static MTLsOptions DefaultMTLsOptions() {
        MTLsOptions options = new MTLsOptions();
        options.serverName = "localhost";
        options.pKey = "./tools/mtls/4_client/private/localhost.key.pem";
        options.certificate = "./tools/mtls/4_client/certs/localhost.cert.pem";
        options.clientCAs = "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem";
        return options;
    }

    public MTLsOptions WithServerName(String serverName) {
        this.serverName = serverName;
        return this;
    }

    public MTLsOptions WithPKey(String pKey) {
        this.pKey = pKey;
        return this;
    }

    public MTLsOptions WithCertificate(String certificate) {
        this.certificate = certificate;
        return this;
    }

    public MTLsOptions WithClientCAs(String clientCAs) {
        this.clientCAs = clientCAs;
        return this;
    }

}
