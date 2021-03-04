/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package io.codenotary.immudb4j.crypto;

import org.bouncycastle.asn1.*;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.ec.CustomNamedCurves;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.math.ec.FixedPointUtil;
import org.bouncycastle.util.Properties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.PublicKey;


public class ECDSASignature {

    public final BigInteger r, s;

    private static final X9ECParameters CURVE_PARAMS = CustomNamedCurves.getByName("secp256k1");
    private static final ECDomainParameters CURVE;

    static {
        // Tell Bouncy Castle to precompute data that's needed during "secp256k1" calculations.
        FixedPointUtil.precompute(CURVE_PARAMS.getG());
        CURVE = new ECDomainParameters(CURVE_PARAMS.getCurve(), CURVE_PARAMS.getG(), CURVE_PARAMS.getN(),
                CURVE_PARAMS.getH());
    }

    public ECDSASignature(BigInteger r, BigInteger s) {
        this.r = r;
        this.s = s;
    }

    public byte[] encodeToDER() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(72);
            DERSequenceGenerator seq = new DERSequenceGenerator(bos);
            seq.addObject(new ASN1Integer(r));
            seq.addObject(new ASN1Integer(s));
            seq.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);  // Cannot happen.
        }
    }

    public static boolean checkSignature(byte[] data, byte[] signature, PublicKey pubKey) {
        ECDSASigner signer = new ECDSASigner();
        ECPublicKeyParameters params = new ECPublicKeyParameters(
                CURVE.getCurve().decodePoint(pubKey.getEncoded()),
                CURVE
        );
        signer.init(false, params);
        try {
            ASN1InputStream decoder = new ASN1InputStream(signature);
            DERSequence seq = (DERSequence) decoder.readObject();
            ASN1Integer r = (ASN1Integer) seq.getObjectAt(0);
            ASN1Integer s = (ASN1Integer) seq.getObjectAt(1);
            decoder.close();
            return signer.verifySignature(data, r.getValue(), s.getValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ECDSASignature decodeFromDER(byte[] bytes) throws Exception {
        ASN1InputStream decoder = null;
        try {
            // BouncyCastle by default is strict about parsing ASN.1 integers. We relax this check, because some
            // Bitcoin signatures would not parse.
            Properties.setThreadOverride("org.bouncycastle.asn1.allow_unsafe_integer", true);
            decoder = new ASN1InputStream(bytes);
            final ASN1Primitive seqObj = decoder.readObject();
            if (seqObj == null)
                throw new Exception("Reached past end of ASN.1 stream.");
            if (!(seqObj instanceof DLSequence))
                throw new Exception("Read unexpected class: " + seqObj.getClass().getName());
            final DLSequence seq = (DLSequence) seqObj;
            ASN1Integer r, s;
            try {
                r = (ASN1Integer) seq.getObjectAt(0);
                s = (ASN1Integer) seq.getObjectAt(1);
            } catch (ClassCastException e) {
                throw new Exception(e);
            }
            // OpenSSL deviates from the DER spec by interpreting these values as unsigned, though they should not be
            // Thus, we always use the positive versions. See: http://r6.ca/blog/20111119T211504Z.html
            return new ECDSASignature(r.getPositiveValue(), s.getPositiveValue());
        } catch (IOException e) {
            throw new Exception(e);
        } finally {
            if (decoder != null)
                try {
                    decoder.close();
                } catch (IOException ignored) {
                }
            Properties.removeThreadOverride("org.bouncycastle.asn1.allow_unsafe_integer");
        }
    }

}
