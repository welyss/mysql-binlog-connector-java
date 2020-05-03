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
import java.util.logging.Logger;

public class Authenticator {
    private final GreetingPacket greetingPacket;
    private final PacketChannel channel;
    private final String schema;
    private final String username;
    private final String password;

    private final Logger logger = Logger.getLogger(getClass().getName());

    public Authenticator(
        GreetingPacket greetingPacket,
        PacketChannel channel,
        String schema,
        String username,
        String password
    ) {
       this.greetingPacket = greetingPacket;
       this.channel = channel;
       this.schema = schema;
       this.username = username;
       this.password = password;
    }

    public void authenticate() throws IOException {
        logger.log(Level.INFO, "Begin auth for " + username);
        int collation = greetingPacket.getServerCollation();

        Command authenticateCommand = "caching_sha2_password".equals(greetingPacket.getPluginProvidedData()) ?
            new AuthenticateSHA2Command(schema, username, password, greetingPacket.getScramble(), collation) :
            new AuthenticateSecurityPasswordCommand(schema, username, password, greetingPacket.getScramble(), collation);

        channel.write(authenticateCommand);
        byte[] authenticationResult = channel.read();
        if (authenticationResult[0] != (byte) 0x00 /* ok */) {
            if (authenticationResult[0] == (byte) 0xFF /* error */) {
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
            } else if (authenticationResult[0] == (byte) 0xFE) {
                switchAuthentication(authenticationResult);
            } else if (authenticationResult[0] == (byte) 0x01) {
                if (authenticationResult.length >= 2 && (authenticationResult[1] == 3) || (authenticationResult[1] == 4)) {
                    // 8.0 auth ok
                    byte[] authenticationResultSha2 = channel.read();
                } else {
                    throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + "&" + authenticationResult[1] + ")");
                }
            } else {
                throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
            }
        }
        logger.log(Level.INFO, "Auth complete " + username);
    }

    private void switchAuthentication(byte[] authenticationResult) throws IOException {
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
            channel.write(switchCommand);
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
