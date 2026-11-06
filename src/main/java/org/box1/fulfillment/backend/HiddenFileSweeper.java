package org.box1.fulfillment.backend;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.erp1.Erp1Names;
import org.box1.fulfillment.erp1.Erp1Reader;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * ERP1 section 4: a name in {@code outbound/} that is neither ready nor {@code .<name>.tmp} is quarantined and moved to
 * {@code consumed/}. Camel's SFTP consumer never lists names starting with a dot, so this sweep handles those names
 * (for example {@code .notes}). Same key, same strict host-key checking, at most the ERP1 maximum plus one byte read.
 */
@ApplicationScoped
public class HiddenFileSweeper {
    private static final Logger LOG = Logger.getLogger(HiddenFileSweeper.class);

    private final LegacyResultService results;
    private final String host;
    private final int port;
    private final String user;
    private final String keyFile;
    private final String knownHosts;
    private final int timeoutMillis;

    public HiddenFileSweeper(LegacyResultService results, @ConfigProperty(name = "fulfillment.sftp.host") String host,
            @ConfigProperty(name = "fulfillment.sftp.port") int port,
            @ConfigProperty(name = "fulfillment.sftp.username") String user,
            @ConfigProperty(name = "fulfillment.sftp.private-key-file") String keyFile,
            @ConfigProperty(name = "fulfillment.sftp.known-hosts-file") String knownHosts,
            @ConfigProperty(name = "fulfillment.sftp.timeout-ms") int timeoutMillis) {
        this.results = results;
        this.host = host;
        this.port = port;
        this.user = user;
        this.keyFile = keyFile;
        this.knownHosts = knownHosts;
        this.timeoutMillis = timeoutMillis;
    }

    /** Names the main result route cannot see and must still quarantine. */
    static boolean isHiddenUnexpected(String name) {
        return name.startsWith(".") && !".".equals(name) && !"..".equals(name)
                && Erp1Names.classify("outbound", name) != Erp1Names.Disposition.IGNORE;
    }

    public int sweep() throws JSchException, SftpException {
        JSch jsch = new JSch();
        jsch.addIdentity(keyFile);
        jsch.setKnownHosts(knownHosts);
        Session session = jsch.getSession(user, host, port);
        session.setConfig("StrictHostKeyChecking", "yes");
        session.setTimeout(timeoutMillis);
        session.connect(timeoutMillis);
        int handled = 0;
        try {
            ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(timeoutMillis);
            try {
                List<ChannelSftp.LsEntry> hidden = new ArrayList<>();
                for (Object o : (Vector<?>) sftp.ls("outbound")) {
                    ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;
                    if (entry.getAttrs().isReg() && isHiddenUnexpected(entry.getFilename())) {
                        hidden.add(entry);
                    }
                }
                for (ChannelSftp.LsEntry entry : hidden) {
                    handled += quarantine(sftp, entry) ? 1 : 0;
                }
            } finally {
                sftp.disconnect();
            }
        } finally {
            session.disconnect();
        }
        return handled;
    }

    private boolean quarantine(ChannelSftp sftp, ChannelSftp.LsEntry entry) {
        String name = entry.getFilename();
        String consumed = LegacyResultService.consumedName(safeName(name));
        try {
            byte[] head;
            try (InputStream in = sftp.get("outbound/" + name)) {
                head = in.readNBytes(Erp1Reader.MAX_FILE_BYTES + 1);
            }
            // Commit first, then move: a crash in between re-records nothing new (receipts are unique per bytes).
            results.quarantine(name, consumed, head, entry.getAttrs().getSize(), "UNEXPECTED_NAME");
            sftp.rename("outbound/" + name, "consumed/" + consumed);
            return true;
        } catch (SftpException | java.io.IOException e) {
            LOG.warnv("Could not quarantine a hidden outbound file: {0}", e.getClass().getSimpleName());
            return false;
        }
    }

    /** Same rule as the result route: a plain, bounded name with no leading dot. */
    static String safeName(String name) {
        String plain = name.replaceAll("[^A-Za-z0-9._-]", "_");
        if (plain.startsWith(".")) {
            plain = "_" + plain.substring(1);
        }
        return plain.length() > 200 ? plain.substring(0, 200) : plain;
    }
}
