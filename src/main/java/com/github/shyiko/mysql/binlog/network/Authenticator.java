package com.github.shyiko.mysql.binlog.network;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.protocol.ErrorPacket;
import com.github.shyiko.mysql.binlog.network.protocol.GreetingPacket;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateNativePasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSHA2Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSecurityPasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.SSLRequestCommand;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;

public class Authenticator {
    private final GreetingPacket greetingPacket;
    private final PacketChannel channel;

    public Authenticator(GreetingPacket greetingPacket, PacketChannel channel) {
       this.greetingPacket = greetingPacket;
       this.channel = channel;
    }

    private void authenticate(GreetingPacket greetingPacket) throws IOException {
        int collation = greetingPacket.getServerCollation();
        int packetNumber = 1;

        boolean usingSSLSocket = false;
        if (sslMode != SSLMode.DISABLED) {
            boolean serverSupportsSSL = (greetingPacket.getServerCapabilities() & ClientCapabilities.SSL) != 0;
            if (!serverSupportsSSL && (sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA ||
                sslMode == SSLMode.VERIFY_IDENTITY)) {
                throw new IOException("MySQL server does not support SSL");
            }
            if (serverSupportsSSL) {
                SSLRequestCommand sslRequestCommand = new SSLRequestCommand();
                sslRequestCommand.setCollation(collation);
                channel.write(sslRequestCommand, packetNumber++);
                SSLSocketFactory sslSocketFactory =
                    this.sslSocketFactory != null ?
                        this.sslSocketFactory :
                        sslMode == SSLMode.REQUIRED || sslMode == SSLMode.PREFERRED ?
                            DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY :
                            DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY;
                channel.upgradeToSSL(sslSocketFactory,
                    sslMode == SSLMode.VERIFY_IDENTITY ? new TLSHostnameVerifier() : null);
                usingSSLSocket = true;
            }
        }

        logger.log(Level.INFO, greetingPacket.getPluginProvidedData());

        Command authenticateCommand = "caching_sha2_password".equals(greetingPacket.getPluginProvidedData()) ?
            new AuthenticateSHA2Command(schema, username, password, greetingPacket.getScramble(), collation) :
            new AuthenticateSecurityPasswordCommand(schema, username, password, greetingPacket.getScramble(), collation);

        channel.write(authenticateCommand, packetNumber);
        byte[] authenticationResult = channel.read();
        if (authenticationResult[0] != (byte) 0x00 /* ok */) {
            if (authenticationResult[0] == (byte) 0xFF /* error */) {
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
            } else if (authenticationResult[0] == (byte) 0xFE) {
                switchAuthentication(authenticationResult, usingSSLSocket);
            } else if (authenticationResult[0] == (byte) 0x01) {
                if (authenticationResult.length >= 2 && (authenticationResult[1] == 3) || (authenticationResult[1] == 4)) {
                    // 8.0 auth ok
                    byte[] authenticationResultSha2 = channel.read();
                    logger.log(Level.FINEST, "SHA2 auth result {0}", authenticationResultSha2);
                } else {
                    throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + "&" + authenticationResult[1] + ")");
                }
            } else {
                throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
            }
        }
    }

    private void switchAuthentication(byte[] authenticationResult, boolean usingSSLSocket) throws IOException {
        /*
            Azure-MySQL likes to tell us to switch authentication methods, even though
            we haven't advertised that we support any.  It uses this for some-odd
            reason to send the real password scramble.
        */
        ByteArrayInputStream buffer = new ByteArrayInputStream(authenticationResult);
        buffer.read(1);

        String authName = buffer.readZeroTerminatedString();
        if ("mysql_native_password".equals(authName)) {
            String scramble = buffer.readZeroTerminatedString();

            Command switchCommand = new AuthenticateNativePasswordCommand(scramble, password);
            channel.write(switchCommand, (usingSSLSocket ? 4 : 3));
            byte[] authResult = channel.read();

            if (authResult[0] != (byte) 0x00) {
                byte[] bytes = Arrays.copyOfRange(authResult, 1, authResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
            }
        } else {
            throw new AuthenticationException("Unsupported authentication type: " + authName);
        }
    }


}
