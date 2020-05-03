package com.github.shyiko.mysql.binlog.network;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;
import com.github.shyiko.mysql.binlog.network.protocol.ErrorPacket;
import com.github.shyiko.mysql.binlog.network.protocol.GreetingPacket;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateNativePasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSHA2Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateSecurityPasswordCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.ByteArrayCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.Command;
import com.github.shyiko.mysql.binlog.network.protocol.command.SSLRequestCommand;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Authenticator {
    private enum AuthMethod {
        NATIVE,
        CACHING_SHA2
    };

    private final GreetingPacket greetingPacket;
    private final PacketChannel channel;
    private final String schema;
    private final String username;
    private final String password;

    private final Logger logger = Logger.getLogger(getClass().getName());

    private final String SHA2_PASSWORD = "caching_sha2_password";
    private final String MYSQL_NATIVE = "mysql_native_password";

    private AuthMethod authMethod = AuthMethod.NATIVE;

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

        Command authenticateCommand;
        if ( SHA2_PASSWORD.equals(greetingPacket.getPluginProvidedData()) ) {
            authMethod = AuthMethod.CACHING_SHA2;
            authenticateCommand = new AuthenticateSHA2Command(schema, username, password, greetingPacket.getScramble(), collation);
        } else {
            authMethod = AuthMethod.NATIVE;
            authenticateCommand = new AuthenticateSecurityPasswordCommand(schema, username, password, greetingPacket.getScramble(), collation);
        }

        channel.write(authenticateCommand);
        readResult();
        logger.log(Level.INFO, "Auth complete " + username);
    }

    private void readResult() throws IOException {
        byte[] authenticationResult = channel.read();
        switch(authenticationResult[0]) {
            case (byte) 0x00:
                // success
                return;
            case (byte) 0xFF:
                // error
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
            case (byte) 0xFE:
                switchAuthentication(authenticationResult);
                return;
            default:
                if ( authMethod == AuthMethod.NATIVE )
                    throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
                else
                    processCachingSHA2Result(authenticationResult);
        }
    }

    private void processCachingSHA2Result(byte[] authenticationResult) throws IOException {
        if (authenticationResult.length < 2)
            throw new AuthenticationException("caching_sha2_password response too short!");

        ByteArrayInputStream stream = new ByteArrayInputStream(authenticationResult);
        stream.readPackedInteger(); // throw away length, always 1

        switch(stream.read()) {
            case 0x03:
                // successful fast authentication
                return;
            case 0x04:
                // need to send continue auth.
                if ( channel.isSSL() ) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    buffer.write(password.getBytes());
                    buffer.write(0);

                    Command c = new ByteArrayCommand(buffer.toByteArray());
                    channel.write(c);
                    readResult();
                } else {
                    throw new AuthenticationException("Please enable SSL in order to support caching_sha2_password auth");
                }
        }
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
        if (MYSQL_NATIVE.equals(authName)) {
            authMethod = AuthMethod.NATIVE;

            String scramble = buffer.readZeroTerminatedString();

            Command switchCommand = new AuthenticateNativePasswordCommand(scramble, password);
            channel.write(switchCommand);
        } else if ( SHA2_PASSWORD.equals(authName) ) {
            authMethod = AuthMethod.CACHING_SHA2;

            String scramble = buffer.readZeroTerminatedString();
            Command authCommand = new AuthenticateSHA2Command(password, scramble);
            channel.write(authCommand);
        }

        readResult();
    }
}
