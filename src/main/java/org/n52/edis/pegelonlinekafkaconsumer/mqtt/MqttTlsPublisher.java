package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import javax.net.ssl.*;
import java.io.FileReader;
import java.io.IOException;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

public class MqttTlsPublisher extends MqttPublisher implements MqttCallback, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttTlsPublisher.class);

    private Tls tls;

    public MqttTlsPublisher() {
        super();
    }

    public MqttTlsPublisher(MqttMessageDeliveryMonitor monitor) {
        super(monitor);
    }

    public static class Tls {

        private boolean peerVerificationEnabled;

        private String caCertFile;

        private String clientCertFile;

        private String keyFile;

        private String password;

        public boolean isPeerVerificationEnabled() {
            return peerVerificationEnabled;
        }

        public void setPeerVerificationEnabled(boolean peerVerificationEnabled) {
            this.peerVerificationEnabled = peerVerificationEnabled;
        }

        public String getCaCertFile() {
            return caCertFile;
        }

        public void setCaCertFile(String caCertFile) {
            this.caCertFile = caCertFile;
        }

        public String getClientCertFile() {
            return clientCertFile;
        }

        public void setClientCertFile(String clientCertFile) {
            this.clientCertFile = clientCertFile;
        }

        public String getKeyFile() {
            return keyFile;
        }

        public void setKeyFile(String keyFile) {
            this.keyFile = keyFile;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public Tls getTls() {
        return tls;
    }

    public void setTls(Tls tls) {
        this.tls = tls;
    }


    @Override
    protected MqttConnectOptions createMqttConnectOptions()  {
        MqttConnectOptions options = applyCommonConnectOptions(new MqttConnectOptions());

        try {
            options.setSocketFactory(getSocketFactory(tls.getCaCertFile(), tls.getClientCertFile(),
                    tls.getKeyFile(), tls.getPassword()));
        } catch (GeneralSecurityException | IOException ex) {
            LOGGER.error("Cannot create SSLSocketFactory, due to {}. Fallback connect options will be created " +
                    "without SSL support.", ex.getMessage());
            LOGGER.debug("Error while creating SSLSocketFactory.", ex);
        }

        return options;
    }


    private SSLSocketFactory getSocketFactory(final String caCrtFile, final String crtFile, final String keyFile,
                                             final String password) throws GeneralSecurityException, IOException {
        Security.addProvider(new BouncyCastleProvider());
        JcaX509CertificateConverter certificateConverter = new JcaX509CertificateConverter().setProvider("BC");

        TrustManager[] trustManagers = createTrustManager(caCrtFile, certificateConverter);

        KeyManager[] keyManagers = null;
        if (tls.isPeerVerificationEnabled()) {
            keyManagers = createKeyManager(crtFile, keyFile, password, certificateConverter);
        }

        // Create an SSL SocketFactory, which will be used for instatiating a TLS connection.
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(keyManagers, trustManagers, null);

        return context.getSocketFactory();
    }

    private TrustManager[] createTrustManager(String caCrtFile, JcaX509CertificateConverter certificateConverter)
            throws GeneralSecurityException, IOException {
        // Load CA certificate
        X509CertificateHolder caCertHolder = (X509CertificateHolder) readPEMFile(caCrtFile);
        X509Certificate caCert = certificateConverter.getCertificate(caCertHolder);

        // Add CA certificate to KeyStore and TrustManager. This controls, which server certificates will be trusted
        KeyStore caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        caKeyStore.load(null, null);
        caKeyStore.setCertificateEntry("ca-certificate", caCert);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(caKeyStore);

        return trustManagerFactory.getTrustManagers();

    }

    private KeyManager[] createKeyManager(String crtFile, String keyFile, String password,
                                          JcaX509CertificateConverter certificateConverter)
            throws GeneralSecurityException, IOException {
        // Load client certificate
        X509CertificateHolder certHolder = (X509CertificateHolder) readPEMFile(crtFile);
        X509Certificate cert = certificateConverter.getCertificate(certHolder);

        //Load client key
        Object keyObject = readPEMFile(keyFile);
        PrivateKey privateKey;
        JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter().setProvider("BC");

        if (keyObject instanceof PEMEncryptedKeyPair) {
            PEMDecryptorProvider provider = new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
            KeyPair keyPair = keyConverter.getKeyPair(((PEMEncryptedKeyPair) keyObject).decryptKeyPair(provider));
            privateKey = keyPair.getPrivate();
        } else if (keyObject instanceof PEMKeyPair) {
            KeyPair keyPair = keyConverter.getKeyPair((PEMKeyPair) keyObject);
            privateKey = keyPair.getPrivate();
        } else if (keyObject instanceof PrivateKeyInfo) {
            privateKey = keyConverter.getPrivateKey((PrivateKeyInfo) keyObject);
        } else {
            throw new IOException(String.format("Unsupported type of private key %s", keyObject.getClass().getCanonicalName()));
        }

        // Add client key and certificate to KeyStore and KeyManager. This controls, which certificates will be
        // sent to the server during peer verification
        KeyStore clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        clientKeyStore.load(null, null);
        clientKeyStore.setCertificateEntry("certificate", cert);
        clientKeyStore.setKeyEntry("private-key", privateKey, password.toCharArray(),
                new Certificate[]{cert});

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeyStore, password.toCharArray());
        return keyManagerFactory.getKeyManagers();
    }

    private static Object readPEMFile(String filePath) throws IOException {
        try (PEMParser reader = new PEMParser(new FileReader(filePath))) {
            return reader.readObject();
        }
    }
}
