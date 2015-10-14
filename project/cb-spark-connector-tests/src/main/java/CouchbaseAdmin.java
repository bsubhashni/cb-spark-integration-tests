import java.io.*;
import com.jcraft.jsch.*;
/**
 * Created by Subhashni on 9/28/15.
 */
public class CouchbaseAdmin {
    private ChannelExec execChannel;
    private Session session;

    public CouchbaseAdmin(String master, String user, String password) {
        try {
            JSch jSch = new JSch();
            String host = master;
            Session session = jSch.getSession(user, host, 22);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.setPassword(password);
            session.connect();
            this.execChannel = (ChannelExec) session.openChannel("exec");
        }
        catch(Exception ex) {
            System.out.print("Caught exception" +ex.getMessage());
        }
    }

    public void runCommand(String command) {
        System.out.println("command" + command);
        this.execChannel.setCommand(command.getBytes());
        try {
            InputStream stdin = this.execChannel.getInputStream();
            this.execChannel.connect();
            byte[] tmp=new byte[1024];
            Thread.sleep(5000);
            while(true){
                while(stdin.available()>0){
                    int i=stdin.read(tmp, 0, 1024);
                    if(i<0)break;
                    System.out.print(new String(tmp, 0, i));
                }
                if(this.execChannel.isClosed()){
                    if(stdin.available()>0) continue;
                    System.out.println("exit-status: "+ this.execChannel.getExitStatus());
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){ throw ee; }
            }

        } catch(Exception ex) {
            System.out.print("Unable to run command " + ex.getMessage());
            execChannel.disconnect();
            session.disconnect();
            System.exit(1);
        }

    }

}