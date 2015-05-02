package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

public class SimpleDhtProvider extends ContentProvider {

	/* 
	*create your data base for storing messages and accesing those messages.
	*/
	//String Message[]=new String[8];
	String myport;
	static final int MESSAGE_KEY=0;
	static final int MESSAGE_VALUE=1;
	static final int MESSAGE_TYPE=2;
	static final int MSG_TEXT=3;
	static final int MYPORT=4;
	static final int SENDPORT=5;
	static final int PRED=6;
	static final int SUCC=7;
	String succesor=null;
	Object lock;
	String R_key;
	String R_value;
	static int s_port = 10000;
	String hashgen;
	String predecessor=null;
	String Final_String;
	//static final int hash=8;
	HashMap<String ,String > PortList;
	private SQLiteDatabase database;
	static String database_name="MYDB";
	static String msg_table="MSGTABLE";
	static String node_table="NODETABLE";
	static final create_table="CREATE TABLE " + msg_table + " ( key STRING PRIMARY KEY," + " value STRING )";
	protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

    /*
     * Instantiates an open helper for the provider's SQLite data repository
     * Do not do database creation and upgrade here.
     */
    MainDatabaseHelper(Context context) {
        super(context, database_name, null, 2);
    }
	public void onCreate(SQLiteDatabase database) {

        // Creates the main table
        database.execSQL(SQL_CREATE_MAIN);
    }
}
	public String prepareMessage(String[] str){
        StringBuilder sb = new StringBuilder("");

        for(String s: str){
            sb.append(s);
            sb.append("#");
        }
//        Log.i(TAG,sb.toString());
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
	public String[] dissectMessage(String s){
        return s.split("#");
    }
	public String[] dissectMessage_final(String s){
        return s.split("**");
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
		if(selection.equals("\"@\"")){
			/*
			*delete your local database
			*/
			database.delete(msg_table,null,null);
		}
			
		else if(selection.equals("\"*\"")){
			database.delete(msg_table,null,null);
			/*
			* We have to delete database from every node, so forward the message telling them to delete.
			*/
			String MESSAGE[]=new String[8];
			MESSAGE[MESSAGE_TYPE]="total_deletion";
			MESSAGE[MYPORT]=myport;
			MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);
			String MSGtoSEND=prepareMessage(MESSAGE);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);			
		}
			
		else{
			/*
			* Look for the key given, in the table, if you can find it, remove the key else go to your succesor and let him check.
			*/
			Cursor position = database.rawQuery("SELECT * from MSGTABLE where key = ?",new String[]{selection});
			if(position!= null && position.getCount()>0)
			{
				database.delete(msg_table,"key='"+selection+"'",null);
			}
			else{
				String MESSAGE[]=new String[8];
				MESSAGE[MESSAGE_KEY]=selection;
				MESSAGE[MESSAGE_TYPE]="Local_deletion";
				MESSAGE[MYPORT]=myport;
				MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);
				String MSGtoSEND=prepareMessage(MESSAGE);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);	
			}
			
		}
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
		String Insert_hash = genHash((String)values.get("key"));
        String value = (String)values.get("value");
        String key = (String)values.get("key");
		 if(hashgen.equals(predecessor)){
               database.insertWithOnConflict(TABLE_NAME,"",values,SQLiteDatabase.CONFLICT_REPLACE);
		 }
            else if(predecessor.compareTo(hashgen)<0){
                if(Insert_hash.compareTo(hashgen)<=0 && Insert_hash.compareTo(predecessor)>0){
                    database.insertWithOnConflict(TABLE_NAME,"",values,SQLiteDatabase.CONFLICT_REPLACE);
                }
                else{
					String[] MsgInsert=new String[8];
                    MsgInsert[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(successor))*2);
                    MsgInsert[MESSAGE_KEY]=(String)values.get("key");
					MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
                    MsgInsert[MESSAGE_TYPE]="insert";
					String MSGtoSEND=prepareMessage(MsgInsert);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
                }
            }
            else if(predecessor.compareTo(hashgen)>0){
                if(Insert_hash.compareTo(predecessor)>0 || Insert_hash.compareTo(hashgen)<=0){
                    database.insertWithOnConflict(TABLE_NAME,"",values,SQLiteDatabase.CONFLICT_REPLACE);
                }
                else{
					String[] MsgInsert=new String[8];
                    MsgInsert[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(successor))*2);
                    MsgInsert[MESSAGE_KEY]=(String)values.get("key");
					MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
                    MsgInsert[MESSAGE_TYPE]="insert";
                   String MSGtoSEND=prepareMessage(MsgInsert);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
                }
            }
            else{
            }

		// this catches might have a problem: PROBLEM
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return null;
    }
	
	
	ArrayList<String> nodes;
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
		 TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2))
		lock=new Object();
		try{
			hashgen= genHash(Integer.toString(Integer.parseInt(myport) / 2));
            predecessor = hashgen;
            succesor = hashgen;
		}
		catch(NoSuchAlgorithmException e)
		{
			// put something if needed later on.
		}
		Context context = getContext();
		mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
        MainDatabaseHelper dbHelper = new MainDatabaseHelper(context);
        database = dbHelper.getWritableDatabase();
		PortList=new HashMap<>(5);
		for(int i=0;i<5;i++)
		{
			int port_num=5554+(i*2);
			String port_num_str=Integer.toString(port_num);
			try{
	            PortList.put(genHash(port_num_str),port_num_str);

			}
			catch(NoSuchAlgorithmException e)
			{
				// put something if needed later on.
			}
		}
		try
        {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,new ServerSocket(s_port));
        }
        catch (IOException e){
        }
		 // "11108" :Create List and put your value, else send it to 11108.
		if(myport.equals("11108")
		{
			nodes=new ArrayList<String>();
			try{
	           String hash_node=genHash("5554");
			   nodes.add(hash_node);
			}
			catch(NoSuchAlgorithmException e)
			{
				// put something if needed later on.
			}
			
		}
		else
		{
			String MESSAGE[]=new String[8];
			MESSAGE[MESSAGE_KEY]=selection;
			MESSAGE[MESSAGE_TYPE]="node_join";
			MESSAGE[MYPORT]=myport;
			MESSAGE[SENDPORT]="11108";
			String MSGtoSEND=prepareMessage(MESSAGE);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);	
		}
        return true;
    }
	private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgToDeliver = dissectMessage(msgs[0]);
                    try{
                        ObjectOutputStream PW;
                        socket = new Socket();
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToDeliver[SENDPORT]));
                        String msgToSend = msgs[0];
                        PW = new ObjectOutputStream(socket.getOutputStream());
                        PW.writeObject(msgToSend);
                        PW.flush();
                        PW.close();
                        socket.close();
                    }catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException to the remote port "+port[i]);

                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException to the remote port "+port[i]);
                        failureHandle(port[i]);
                        timer.cancel();

                    }


                }
			}

	 private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ObjectInputStream ISR=null;
            while (true) {
                try {
                    socketonReceive = serverSocket.accept();
                    ISR = new ObjectInputStream(socketonReceive.getInputStream());
                    String msg=(String)ISR.readObject();
                    onProgressUpdate(msg);
					ISR.close();
					socketonReceive.close();

                } catch (IOException ex) {
                    Log.e(TAG,"IO Exception in Server Side");
                    timer.cancel();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    try{
                        socketonReceive.close();
                        ISR.close();
                    }
                    catch (IOException e)
                    {
                        Log.e(TAG,"IO exception while closing the server side.");
                    }

                }


            }
        }
		 protected void onProgressUpdate(final String...strings) {
			final String strReceived = strings[0];
            Log.i(TAG,"Received: "+strReceived);
            String[] message = dissectMessage(strReceived);
			/*
			*	deal with delete all, delete one or query all and query one, insert one, joins
			*/
			switch(message[MESSAGE_TYPE])
			{
				case Local_deletion:
					Cursor position = database.rawQuery("SELECT * from MSGTABLE where key = ?",new String[]{message[MESSAGE_KEY]});
					if(position!= null && position.getCount()>0)
					{
						database.delete(msg_table,"key='"+message[MESSAGE_KEY]+"'",null);
					}
					else{
						message[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);
						String MSGtoSEND=prepareMessage(message);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);	
					}
				break;
				case total_deletion:
					if(myport.equals(message[MYPORT]))
					{
						
					}
					else
					{
					database.delete(msg_table,null,null);
					message[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);
					String MSGtoSEND=prepareMessage(message);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);	
					}
				break;
				case node_join:
					String node_entry= message[MYPORT];
					int temp=Integer.parseInt(node_entry);
					temp=temp/2;
					node_entry=Integer.toString(temp);
					String hash=genHash(node_entry);
					nodes.add(hash);
					Collections.sort(nodes);
					int index=nodes.IndexOf(hash);
					String Msg[]=new String[8];
					Msg[MESSAGE_TYPE]="node_join_reply";
					if(index==(nodes.size()-1))
					{
					Msg[PRED]=nodes.get(index-1);
					Msg[SUCC]=nodes.get(0);
					Msg[MYPORT]=myport;
					Msg[SENDPORT]=message[MYPORT];	
					
					}
					else if(index==0)
					{
					Msg[PRED]=nodes.get(nodes.size()-1);
					Msg[SUCC]=nodes.get(1);
					Msg[MYPORT]=myport;
					Msg[SENDPORT]=message[MYPORT];

					}
					else{
					Msg[PRED]=nodes.get(index-1);
					Msg[SUCC]=nodes.get(index+1);
					Msg[MYPORT]=myport;
					Msg[SENDPORT]=message[MYPORT];
					}
					
					String MsgforSuc=new String[8];
					
					MsgforSuc[MESSAGE_TYPE]="set_pred";
					MsgforSuc[PRED]=nodes.get(index);
					MsgforSuc[MYPORT]=myport;
					MsgforSuc[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(Msg[SUCC]))*2);					
					String MsgforPred=new String[8];				
					MsgforPred[MESSAGE_TYPE]="set_succ";
					MsgforPred[SUCC]=nodes.get(index);
					MsgforPred[MYPORT]=myport;
					MsgforPred[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(Msg[PRED]))*2);
					String MSG_OWN=prepareMessage(Msg);
					String MSG_SUC=prepareMessage(MsgforSuc);
					String MSG_PRE=prepareMessage(MsgforPred);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSG_OWN, null);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSG_SUC, null);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSG_PRE, null);
					
				break;
				case insert:
							ContentValues cv = new ContentValues();
							cv.put("key",message[MESSAGE_KEY]);
							cv.put("value",message[MESSAGE_VALUE]);
							insert(mUri, cv);
				break;
				case node_join_reply:
					predecessor=message[PRED];
					succesor=message[SUCC];
					
				break;
				case set_pred:
					predecessor=message[PRED];
				break;
				case set_succ:
					succesor=message[SUCC];
				break;
				case query_all:
					if(myport.equals(message[MYPORT]))
					{
						synchronized (lock){
                            Final_String=message[MSG_TEXT];
                            lock.notify();
                        }
					}
					else
					{
                        position = database.rawQuery("SELECT * FROM MSGTABLE",null);
						message[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(successor))*2);
						
                        if(position!=null && position.getCount()>0){
                            position.moveToFirst();
                            for(int i = 0 ; i < position.getCount() ; i++){
								message[MSG_TEXT]=message[MSG_TEXT]+"**"+position.getString(0)+"**"+position.getString(1);
                                position.moveToNext();
                            }
                        }
						String MSGtoSEND=prepareMessage(message);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND, null);	
					}
							 
				
				break;
				case query_one:
                    Cursor position = database.rawQuery("SELECT * FROM MSGTABLE where key = ?", new String[]{message[MESSAGE_KEY]});
                    if(position!=null && position.getCount()>0){
                        position.moveToFirst();
                        message[MYPORT]=myport;
						message[SENDPORT]=message[MYPORT];
						message[MESSAGE_TYPE]="query_resolved";
						message[MSG_TEXT]=position.getString(0)+"**"+position.getString(1);
                        String MSGtoSEND=prepareMessage(message);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND, null);
                    }
                    else{
                       message[SENDPORT]=Integer.toString(Integer.parseInt(hashMap.get(successor))*2);
					   String MSGtoSEND=prepareMessage(message);
                       new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND, null);
                    }
                    break;
				case query_resolved:
						synchronized (lock){
                        Final_String = message[MSG_TEXT];
                        lock.notify();
						}
				break;
				
			}	
		 }
	 }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
		String sel=selection;
		Cursor position = null;
        switch (sel){
            case "\"*\"":
                if(predecessor.compareTo(hashgen)==0)
                {
                    position = database.rawQuery("SELECT * FROM MSGTABLE",null);
                }
                else{
                    Log.i(TAG, "Executing * query from me.");
                    {
                        position = database.rawQuery("SELECT * FROM MSGTABLE",null);
						String MESSAGE[]=new String[8];
						MESSAGE[MESSAGE_TYPE]="query_all";
						MESSAGE[MYPORT]=myport;
						MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(successor))*2);
						
                        if(position!=null && position.getCount()>0){
                            position.moveToFirst();
                            for(int i = 0 ; i < position.getCount() ; i++){
								MESSAGE[MSG_TEXT]=position.getString(0)+"**"+position.getString(1);
                                position.moveToNext();
                            }
                        }
						String MSGtoSEND=prepareMessage(MESSAGE);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND, null);
                        synchronized (lock){
                            try {
                                lock.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
							String[] Str_Ans=dissectMessage_final(Final_String);
							int length=Str_Ans.length;
							for(int j=0;j<length;j=j+2)
							{
								 matrixCursor.addRow(new Object[]{Str_Ans[j],Str_Ans[j+1]});
							}
                            position = matrixCursor;
                            position.moveToFirst();
                        }
                    }
                }
			break;	
			case "\"@\"":
                position= database.rawQuery("SELECT * FROM MSGTABLE",null);
                break;
			default:
                position = database.rawQuery("SELECT * FROM MSGTABLE WHERE key = ?",new String[]{selection});
                    if(position==null || position.getCount()==0){
						String MESSAGE[]=new String[8];
						MESSAGE[MESSAGE_TYPE]="query_one";
						MESSAGE[MESSAGE_KEY]=selection;
						MESSAGE[MYPORT]=myport;
						MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(successor))*2);
						String MSGtoSEND=prepareMessage(MESSAGE);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND, null);
						String key = selection;
                        synchronized(lock){
                            try {
                                lock.wait();                                
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
						String[] Res_final=new String[];
						Res_final=dissectMessage_final(Final_String);
                        String value = Res_final[1];
                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
                        matrixCursor.addRow(new Object[]{key,value});
                        position = matrixCursor;
                    }
        }

        return position;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
	private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
