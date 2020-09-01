package il.co.ilrd.gatewayserver;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.mysql.jdbc.ResultSetMetaData;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import il.co.ilrd.http_message.HttpBuilder;
import il.co.ilrd.http_message.HttpParser;
import il.co.ilrd.http_message.HttpStatusCode;
import il.co.ilrd.jarloader.JarLoader;
import il.co.ilrd.observer.Callback;
import il.co.ilrd.observer.Dispatcher;

// JSON:
//{
//	"Commandkey": keyValue,
//	"Data": data (the data can be another json)
//}


public class GatewayServer {
	private ThreadPoolExecutor threadPool;
	private CMDFactory<FactoryCommand, String, Void> cmdFactory = 
											CMDFactory.getFactoryInstance();
	private ConnectionsHandler connectionHandler =  new ConnectionsHandler();
	private MessageHandler messageHandler = new MessageHandler();
	private DbMangeHandler dbHandler = new DbMangeHandler();
	private FactoryCommandLoader factoryCmdLoader;
	
	private final static int DEAFULT_NUM_THREADS = 
								Runtime.getRuntime().availableProcessors();
	
	public GatewayServer(int numOfThreads, String jarDirPath) throws Exception  {
		if(0 < numOfThreads) {
			threadPool = new ThreadPoolExecutor(numOfThreads, numOfThreads, 1, 
						TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		}
		else {
			threadPool = new ThreadPoolExecutor(DEAFULT_NUM_THREADS, 
					DEAFULT_NUM_THREADS, 1, TimeUnit.SECONDS, 
								new LinkedBlockingQueue<Runnable>());		
			}
		
		factoryCmdLoader = new FactoryCommandLoader(jarDirPath);
	}
	
	public GatewayServer(String jarDirPath) throws Exception {
		this(DEAFULT_NUM_THREADS, jarDirPath);
	}
	
	public void addHttpServer(ServerPort port) throws Exception {
		checkIfPortValid(port, ServerPort.HTTP_SERVER_PORT);
		
		// ------------- Low Level ------------------
		ServerConnection httpServer = new LowLevelHttpServer();

		// ------------- High Level ------------------
//		ServerConnection httpServer = new HighLevelHttpServer();
		
		connectionHandler.addServer(httpServer);
		
	}
	
	public void addTcpServer(ServerPort port) throws Exception {			
		checkIfPortValid(port, ServerPort.TCP_SERVER_PORT);
		ServerConnection tcpServer = new TcpServer();
		connectionHandler.addServer(tcpServer);
	}
	
	public void addUdpServer(ServerPort port) throws Exception {
		checkIfPortValid(port, ServerPort.UDP_SERVER_PORT);
		ServerConnection udpServer = new UdpServer();
		connectionHandler.addServer(udpServer);
	}
	
	public void start() throws Exception {
		connectionHandler.startConnections();
	}
	
	public void stop() throws IOException {
		connectionHandler.stopConnections();
		factoryCmdLoader.stopWatchJarDir();
	}
	
	public void setNumOfThreads(int numOfThread) {
		threadPool.setCorePoolSize(numOfThread);
	}

	private void checkIfPortValid(ServerPort recivePort, ServerPort realPort) 
															throws Exception {
		if(!recivePort.equals(realPort)) {
			throw new Exception("the port is not valid");
		}
	}
		
	/*---------------------- Connection Handler class ------------------------*/
	
	private class ConnectionsHandler implements Runnable {
		private List<ServerConnection> connectionsBeforeStart = new LinkedList<>();
		private Map<Channel, ServerConnection> registeredConnections = new HashMap<>();
		protected Map<SocketChannel, ClientInfo> socketsInfo = new HashMap<>();
		private ByteBuffer messageBuffer = ByteBuffer.allocate(BUFFER_SIZE);
		private boolean isRunning = false;
		private boolean toContinueRun = false;
		private boolean isResourceClosed = false;
		private Selector selector;
		private HttpServer sunHttpServer;

		private final static int BUFFER_SIZE = 8192;
		private final static int TIMEOUT_SELECT = 5000;

		
		public void addServer(ServerConnection connection) {
				Objects.requireNonNull(connection);
				connectionsBeforeStart.add(connection);
		}

		public void startConnections() throws Exception {
			if(isRunning) {
				throw new Exception("the server start allready.");
			}
			
			registerConnectionsToSelector();
			new Thread(this).start();
			
			if(null != sunHttpServer) {
				sunHttpServer.start();				
			}
		}

		private void registerConnectionsToSelector() throws IOException {
			selector = Selector.open();
			
			for (ServerConnection connection : connectionsBeforeStart) {
				connection.initServerConnection();
				registeredConnections.put(connection.getChannel(), connection);
			}
		}
		
		private void stopConnections() {
			toContinueRun = false;
			
			if(!isResourceClosed) {
				closeResource();
			}
		}
		

		@Override
		public void run() {
			try {
				isRunning = true;
				toContinueRun = true;
				runServer();
			} catch (IOException | ClassNotFoundException e) {
				stopConnections();
				System.out.println("Server- run override function: " + e);
			} 			
		}
		
		private void runServer() throws IOException, ClassNotFoundException {
			try {
				while (toContinueRun) {
					if(0 == selector.select(TIMEOUT_SELECT)) {
						System.out.println("server-running...");
						continue;
					}
					
					Set<SelectionKey> selectedKeys = selector.selectedKeys();
					Iterator<SelectionKey> iter = selectedKeys.iterator();
			
					while (iter.hasNext()) {
						SelectionKey key = iter.next();
	
						if (key.isValid() && key.isAcceptable()) {
							registerTcpClientToSelector(key);
						}
						else if (key.isValid() && key.isReadable()) {
							readableHandler(key);
						}
						
						iter.remove();
					}
				}
			}catch (Exception e) {
				if(toContinueRun) {
					stopConnections();
					e.printStackTrace(); // *******************************************************************************
					System.out.println("runServer: " + e.getMessage());
				}
			}
		}
		
		private void registerTcpClientToSelector(SelectionKey key) 
				throws IOException {
			ServerSocketChannel tcpserverSocket = 
			(ServerSocketChannel) key.channel();
			SocketChannel clientTcp = tcpserverSocket.accept();
			clientTcp.configureBlocking(false);
			clientTcp.register(selector, SelectionKey.OP_READ);
			ServerConnection connection = registeredConnections.get(tcpserverSocket);
			registeredConnections.put(clientTcp, connection);
			socketsInfo.put(clientTcp, new TcpClientInfo(clientTcp, connection));
		}
		
		private void readableHandler(SelectionKey key) 
						throws IOException, ClassNotFoundException {
			Channel currChannel = key.channel();
			registeredConnections.get(currChannel).handleRequestMessage(currChannel);
		}
		
		private void closeResource() {			
			try {
				Set<SelectionKey> selectedKeys = selector.keys();
				
				for (SelectionKey Key : selectedKeys) {
					Key.channel().close();
				}
				
				if(null != sunHttpServer) {
					sunHttpServer.stop(0);
				}
				
				threadPool.shutdownNow();
				
				selector.close();
				isResourceClosed   = true;
			} catch (IOException e) {
				System.out.println("close resource: " + e.getMessage());
			}
		}
		
		private void closeAndRemoveClient(SocketChannel client) throws IOException {
			client.close();
			connectionHandler.registeredConnections.remove(client, this);
		}

		public ByteBuffer getMsgBuffer() {
			return messageBuffer;
		}
		
	}
	
	/*------------------------- Server Connection ----------------------------*/
	
	private interface ServerConnection {
		public void initServerConnection() throws IOException;
		public void handleRequestMessage(Channel channel) throws IOException;
		public void sendResponse(String message, ClientInfo clientInfo) throws IOException;
		public Channel getChannel();
	}
	
	
	/*------------------------- TcpServer class ------------------------------*/

	private class TcpServer implements ServerConnection {
		private ServerSocketChannel tcpServerSocket;

		@Override
		public void initServerConnection() throws IOException  {
			tcpServerSocket = ServerSocketChannel.open();
			tcpServerSocket.bind(new InetSocketAddress(
										ServerPort.TCP_SERVER_PORT.getPort()));
			tcpServerSocket.configureBlocking(false);
			tcpServerSocket.register(connectionHandler.selector, 
														SelectionKey.OP_ACCEPT);		
		}

		@Override
		public void handleRequestMessage(Channel channel) throws IOException 
														 {
			SocketChannel client = (SocketChannel) channel;
			ByteBuffer messageBuffer = connectionHandler.getMsgBuffer();
			messageBuffer.clear();
			
			if(-1 == client.read(messageBuffer)) {
				connectionHandler.closeAndRemoveClient(client);					
			}
			else {
				ClientInfo clientInfo = connectionHandler.socketsInfo.get(client);
				String msg = new String(messageBuffer.array());
				messageHandler.handleMessage(msg, clientInfo);
			}
		}

		@Override
		public void sendResponse(String message, ClientInfo clientInfo) throws IOException {
			SocketChannel client = (SocketChannel) clientInfo.getChannel();
			ByteBuffer messageBuffer = ByteBuffer.wrap(message.getBytes());

			while(messageBuffer.hasRemaining()) {
				client.write(messageBuffer);
			}
		}

		@Override
		public Channel getChannel() {
			return tcpServerSocket;
		}

	}
	
	/*------------------------- UdpServer class ------------------------------*/
	
	private class UdpServer implements ServerConnection {
		private DatagramChannel udpServerDatagram;

		@Override
		public void initServerConnection() throws IOException {
			udpServerDatagram = DatagramChannel.open();
			udpServerDatagram.socket().bind(new InetSocketAddress(
										ServerPort.UDP_SERVER_PORT.getPort()));
			udpServerDatagram.configureBlocking(false);
			udpServerDatagram.register(connectionHandler.selector, 
														SelectionKey.OP_READ);	
		}

		public void handleRequestMessage(Channel channel) throws IOException {
			DatagramChannel client = (DatagramChannel)channel;
			ByteBuffer messageBuffer = connectionHandler.getMsgBuffer();
			messageBuffer.clear();
			SocketAddress clientAddress = client.receive(messageBuffer);
			ClientInfo clientInfo = new UdpClientInfo(client, clientAddress, this);
			
			if(null != clientAddress) {
				String msg = new String(messageBuffer.array());
				messageHandler.handleMessage(msg, clientInfo);
			}
		}

		@Override
		public void sendResponse(String message, ClientInfo clientInfo) throws IOException {
			DatagramChannel client = (DatagramChannel)clientInfo.getChannel();
			ByteBuffer messageBuffer = ByteBuffer.wrap(message.getBytes());

			client.send(messageBuffer, clientInfo.getclientAddress());
			messageBuffer.clear();
		}
		
		@Override
		public Channel getChannel() {
			return udpServerDatagram;
		}
	}
	
	
	/*---------------------- LowLevelHttpServer class ------------------------*/

	private class LowLevelHttpServer implements ServerConnection {
		private ServerSocketChannel tcpServerSocket;
		private HttpParser parser;
		private Map<String, String> headersResponse = new HashMap<>();
		
		private static final String HEADER_LENGTH = "Content-Length";
		private final static String CONTENT_TYPE_HEADER = "Content-Type";
		private final static String APP_JSON = "application/json";
		
		public LowLevelHttpServer() {
			initHeadersResponse();
		}

		private void initHeadersResponse() {
			headersResponse.put(CONTENT_TYPE_HEADER, APP_JSON);
		}
		
		@Override
		public void initServerConnection() throws IOException  {
			tcpServerSocket = ServerSocketChannel.open();
			tcpServerSocket.bind(new InetSocketAddress(
										ServerPort.HTTP_SERVER_PORT.getPort()));
			tcpServerSocket.configureBlocking(false);
			tcpServerSocket.register(connectionHandler.selector, 
														SelectionKey.OP_ACCEPT);		
		}

		@Override
		public void handleRequestMessage(Channel channel) throws IOException {
			SocketChannel client = (SocketChannel) channel;
			ByteBuffer messageBuffer = connectionHandler.getMsgBuffer();
			messageBuffer.clear();
			
			if(-1 == client.read(messageBuffer)) {
				connectionHandler.closeAndRemoveClient(client);					
			}
			else {
				messageBuffer.clear();
				ClientInfo clientInfo = connectionHandler.socketsInfo.get(client);
				String msg = new String(messageBuffer.array());
				parser = new HttpParser(msg);
				messageHandler.handleMessage(parser.getBody().getBody(), clientInfo);
			}
		}

		@Override
		public void sendResponse(String message, ClientInfo clientInfo) throws IOException {
			SocketChannel client = (SocketChannel) clientInfo.getChannel();
			String httpResponseMsg = createResponseMsg(HttpStatusCode.OK, message);
			ByteBuffer messageBuffer = ByteBuffer.wrap(httpResponseMsg.getBytes());
			
			while(messageBuffer.hasRemaining()) {
				client.write(messageBuffer);
			}
		}

		@Override
		public Channel getChannel() {
			return tcpServerSocket;
		}
		
		private String createResponseMsg(HttpStatusCode statusCode, String bodyResMsg) {
			headersResponse.put(HEADER_LENGTH, Integer.toString(bodyResMsg.length()));

			return HttpBuilder.createHttpResponseMessage(
					parser.getStartLine().getHttpVersion(), statusCode, 
					headersResponse, bodyResMsg);
		}

	}
	
	/*---------------------- HighLevelHttpServer class -----------------------*/

	private class HighLevelHttpServer implements ServerConnection {
		private HttpExchange exchangeMsg;
		
		private final static String CONTENT_TYPE_HEADER = "Content-Type";
		private final static String APP_JSON = "application/json";

		@Override
		public void initServerConnection() throws IOException {
			if(null == connectionHandler.sunHttpServer) {
				connectionHandler.sunHttpServer = HttpServer.create(
						new InetSocketAddress(ServerPort.HTTP_SERVER_PORT.getPort()), 0);
				connectionHandler.sunHttpServer.setExecutor(null);
				connectionHandler.sunHttpServer.createContext("/", HandlerSunHttpMsg);
			}			
		}
		
		private HttpHandler HandlerSunHttpMsg = new HttpHandler() {
			@Override
			public void handle(HttpExchange exchangeMsg) throws IOException {
					HighLevelHttpServer.this.exchangeMsg = exchangeMsg;
					handleRequestMessage(null);
			}
		};

		@Override
		public void handleRequestMessage(Channel channel) throws IOException {
			String jsonBody = new String(exchangeMsg.getRequestBody().readAllBytes());
			ClientInfo clientInfo = new HttpClientInfo(null, this);			
			messageHandler.handleMessage(jsonBody, clientInfo);
		}

		@Override
		public void sendResponse(String message, ClientInfo clientInfo) throws IOException {
			Headers headers = exchangeMsg.getResponseHeaders();
			addContentTypeToHeaders(headers);
			exchangeMsg.sendResponseHeaders(HttpStatusCode.OK.getCode(), message.length());
			OutputStream outputStream = exchangeMsg.getResponseBody();
			outputStream.write(message.getBytes()); 
			outputStream.close();
		}

		private void addContentTypeToHeaders(Headers headers) {
			headers.add(CONTENT_TYPE_HEADER, APP_JSON);
		}

		@Override
		public Channel getChannel() {
			return null;
		}
		
	}
	
	
	/*----------------------- Message Handler class --------------------------*/

	private class MessageHandler {
		private JsonToTaskConvertor jsonToRunnable = new JsonToTaskConvertor();
		
		private void handleMessage(String message, ClientInfo clientInfo) 
													throws IOException {
			Runnable task = jsonToRunnable.convertToTask(clientInfo, message);
			threadPool.execute(task);
		}
		
		private void sendMessage(ClientInfo clientInfo, String jsonMsg)  {
			try {
				clientInfo.getConnection().sendResponse(jsonMsg, clientInfo);
			} catch (IOException e) {
				System.out.println("\n\n\n"); //**************************************************8
				e.printStackTrace();		// **************************************************
				System.out.println("sendMsg: " + e.getMessage());
				connectionHandler.stopConnections();
			}
		}
	}

	
	/*------------------- JsonToRunnableConvertor class ----------------------*/

	private class JsonToTaskConvertor {		
		private Gson convertMsg = new Gson();
		private final static String CMD_TYPE = "CommandType";
		private final static String ERR_MSG = "error_message";

		private Runnable convertToTask(ClientInfo clientInfo, String message) {
			MessageElements msgElements = convertJsonToMsgElem(message);
			
			return new IotTask(msgElements.getKey(), msgElements.getData(), clientInfo); 
		}
		
		private MessageElements convertJsonToMsgElem(String message) {
			//System.out.println("msg = " + message + "\n\n\n\n");  ****************************************
			return convertMsg.fromJson(message.trim(), MessageElements.class);
		}
		
		private class IotTask implements Runnable {
			private String commandKey;
			private DataElements data;
			private ClientInfo clientInfo;			
	
			public IotTask(String commandKey, DataElements data, ClientInfo clientInfo) {
				this.commandKey = commandKey;
				this.data = data;
				this.clientInfo = clientInfo;
			}

			@Override
			public void run() {
				try {
					String dbName = data.getDbName();
					dbHandler.createDatabaseIfNeeded(dbName);
					String valueOfMsg = getValueOfJsonMsg(commandKey, data);
					DatabaseManagement dbMangeObj = dbHandler.getDbMangeObj(dbName);
					String responseMsg = cmdFactory.create(commandKey).run(
													valueOfMsg, dbMangeObj);
					String jsonResponse = convertToJsonStr(CMD_TYPE, responseMsg);
					messageHandler.sendMessage(clientInfo, jsonResponse);
					
				} catch (SQLException | JsonSyntaxException e) {   
					System.out.println("Task-run(): " + e.getMessage());
					
					e.printStackTrace(); // ***********************************************************************
					
					String errResponse = convertToJsonStr(ERR_MSG, e.getMessage());
					messageHandler.sendMessage(clientInfo, errResponse);
				}
			}
			
			private String getValueOfJsonMsg(String commandKey, DataElements data) {
				if(commandKey.equals("IOT_UPDATE")) {
					return data.getRawData();
				}
				
				return data.getSqlCommand();
			}

			private String convertToJsonStr(String key, String value) {
				HashMap<String, String> jsonMap = new HashMap<>();
				jsonMap.put(key, value);
				
				return convertMsg.toJson(jsonMap, jsonMap.getClass());
			}
		}
		
		/*--------------------- MessageElements class ------------------------*/

		private class MessageElements {
			private String Commandkey;
			private DataElements Data;
			
			public String getKey() {
				return Commandkey;
			}

			public DataElements getData() {
				return Data;
			}
		}
	}
	
	/*------------------------- DataElements class ---------------------------*/
	
	private class DataElements {
		private String dbName;
		private String sqlCommand;
		private String rawData;
		
		public String getDbName() {
			return dbName;
		}

		public String getSqlCommand() {
			return sqlCommand;
		}

		public String getRawData() {
			return rawData;
		}
	}
	
	/*--------------------- FactoryCommandLoader class -----------------------*/
	
	private class FactoryCommandLoader {
		private Callback<String> callback;
		private Consumer<String> update = (jarFilePath)-> updateFunc(jarFilePath);
		private JarMonitor monitor;
		private Map<String, Double> versionCommandsMap = new HashMap<>();
		
		private static final String JAR_SUFFIX = ".jar";
		private static final String VERSION_METHOD = "getVersion";
		private static final String NAME_METHOD = "getCommandName";
		private final static String FCM_INTERFACE = "FactoryCommandModifier";

		
		private FactoryCommandLoader(String jarDirPath) throws Exception {
			callback = new Callback<>(update, null);
			monitor = new JarMonitor(jarDirPath);
			monitor.register(callback);
			
			loadAllJarsFromDir(jarDirPath);
		}
		
		private void load(String jarFilePath) throws ClassNotFoundException, IOException, 
				NoSuchMethodException, SecurityException, InstantiationException, 
							IllegalAccessException, IllegalArgumentException, 
											InvocationTargetException {
			List<Class<?>> classesList = null;
			classesList = JarLoader.load(FCM_INTERFACE, jarFilePath);
							
			for (Class<?> currentClass : classesList) {
				
				Method versionMethod = currentClass.getMethod(VERSION_METHOD);
				Double newVersion = (Double) versionMethod.invoke(null);
				Method nameMethod = currentClass.getMethod(NAME_METHOD);
				String commandName = (String) nameMethod.invoke(null);

				if(!versionCommandsMap.containsKey(commandName) || 
						versionCommandsMap.get(commandName) < newVersion) {
					addToFactory(currentClass, commandName, newVersion);
					versionCommandsMap.put(commandName, newVersion);
				}
			}
		
		}
		
		private void updateFunc(String jarFilePath) {
			try {
				load(jarFilePath);
			}catch (ClassNotFoundException | IOException | 
					IllegalAccessException | IllegalArgumentException | 
						InvocationTargetException | NoSuchMethodException | 
								SecurityException | InstantiationException e) {
				System.err.println("Loader exeption - " + e);
			}
		}
		
		public void addToFactory(Class<?> currentClass, String commandName, 
							Double newVersion) throws InstantiationException, 
						IllegalAccessException, IllegalArgumentException, 
								InvocationTargetException, NoSuchMethodException, 
													SecurityException {
			FactoryCommandModifier objClass = (FactoryCommandModifier) 
							currentClass.getConstructor().newInstance();
			objClass.addToFactory();
		}
		
		public void stopWatchJarDir() throws IOException {
			monitor.stopUpdate();
		}
		
		public void loadAllJarsFromDir(String dirPath) throws Exception {
			File dirFile = new File(dirPath);
			
			if(!dirFile.isDirectory()) {
				throw new NotDirectoryException("File is not directory.");
			}
			
			try {
				for (File file : dirFile.listFiles()) {
					if(file.getName().endsWith(JAR_SUFFIX)) {
						load(file.getPath());
					}	
				}
			}catch (ClassNotFoundException | IOException | 
					IllegalAccessException | IllegalArgumentException | 
						InvocationTargetException | NoSuchMethodException | 
								SecurityException | InstantiationException e) {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	
	/*-------------------------- JarMonitor class ----------------------------*/

	private class JarMonitor implements DirMonitor {
		private Dispatcher<String> dispatcher = new Dispatcher<>();
		private WatchService watcher = FileSystems.getDefault().newWatchService();
		private File dirToWatch;
		private boolean watcherThreadIsRunning = true;

		
		public JarMonitor(String dirPath) throws IOException {
			Objects.requireNonNull(dirPath);
			dirToWatch = new File(dirPath);
			
			if(!dirToWatch.isDirectory()) {
				throw new NotDirectoryException(dirPath);
			}
			
			startWatch();
		}

		private void startWatch() throws IOException {	
			Path directory = dirToWatch.toPath();
			directory.register(watcher, ENTRY_MODIFY, ENTRY_CREATE);
			new Thread(watchDir).start();
		}
		
		@Override
		public void register(Callback<String> callback) {
			Objects.requireNonNull(callback);
			dispatcher.register(callback);
		}
		
		@Override
		public void unregister(Callback<String> callback) {
			Objects.requireNonNull(callback);
			dispatcher.unregister(callback);	
		}

		@Override
		public void stopUpdate() throws IOException  {
			watcherThreadIsRunning = false;
			watcher.close();
		}
		
		private void updateAll(String changedJarFilePath) {
			dispatcher.updateAll(changedJarFilePath);
		}
		
		private Runnable watchDir = new Runnable() {
			private WatchKey key = null;
			private final static String jarSuffix = ".jar";
			
			@Override
			public void run() {
				
				while (watcherThreadIsRunning) {
					try {
				        key = watcher.take();
				    } catch (ClosedWatchServiceException | InterruptedException e) {
						continue;
				    }
					
					for (WatchEvent<?> event : key.pollEvents()) {
						final Path changedFile = (Path)key.watchable();
						final Path fullPathChangedFile = 
										changedFile.resolve((Path) event.context());
								            
			            if (isJarFile(fullPathChangedFile)) {
			            	updateAll(fullPathChangedFile.toString());
			            }
			        }
					
					if(!key.reset()) {
						watcherThreadIsRunning = false;
					}
				}			
			}

			private boolean isJarFile(Path changedFile) {
				return changedFile.getFileName().toString().endsWith(jarSuffix);
			}
		};
	}
	
	
	/*---------------------- DbMangeHandler class ----------------------------*/	

	private class DbMangeHandler {
		private Map<String, DatabaseManagement> companiesMap = new HashMap<>();
		
		private static final String URL = "jdbc:mysql://localhost?useSSL=false";
		private static final String USER_NAME = "root";
		private static final String PASSWORD = "password";
		
		public Map<String, DatabaseManagement> getCompaniesMap() {
			return companiesMap;
		}
		
		public DatabaseManagement getDbMangeObj(String dbName) {
			return dbHandler.getCompaniesMap().get(dbName);
		}

		public void createDatabaseIfNeeded(String dbName) throws SQLException {
			if(!companiesMap.containsKey(dbName)) {
				DatabaseManagement newDbMangeObj = 
						new DatabaseManagement(URL, USER_NAME, PASSWORD, dbName);
				companiesMap.put(dbName, newDbMangeObj);
			}
		}
	}
	
	
	/*------------------------ ClientInfo interface --------------------------*/
	
	private interface ClientInfo {
		public Channel getChannel();
		public SocketAddress getclientAddress();
		public ServerConnection getConnection();
	}
	
	/*----------------------- TcpClientInfo class ----------------------------*/	
	
	private class TcpClientInfo implements ClientInfo {
		private SocketChannel chennelTCp;
		private ServerConnection connection;
		
		public TcpClientInfo(SocketChannel chennelTCp, 
								ServerConnection connection) {
			this.chennelTCp = chennelTCp;
			this.connection = connection;
		}
		
		@Override
		public Channel getChannel() {
			return chennelTCp;
		}

		@Override
		public SocketAddress getclientAddress() {
			return null;
		}

		@Override
		public ServerConnection getConnection() {
			return connection;
		}
	}
	
	
	/*----------------------- UdpClientInfo class ----------------------------*/	
	
	private class UdpClientInfo implements ClientInfo {
		private DatagramChannel udpServer;
		private SocketAddress clientAddress;
		private ServerConnection connection;
		
		public UdpClientInfo(DatagramChannel udpServer, 
					SocketAddress clientAddress, ServerConnection connection) {
			this.udpServer = udpServer;
			this.clientAddress = clientAddress;
			this.connection = connection;
		}
		
		@Override
		public Channel getChannel() {
			return udpServer;
		}


		@Override
		public SocketAddress getclientAddress() {
			return clientAddress;
		}

		@Override
		public ServerConnection getConnection() {
			return connection;
		}
	}
	
	/*---------------------- HttpClientInfo class ----------------------------*/	
	
	private class HttpClientInfo implements ClientInfo {
		private SocketChannel chennelTCp;
		private ServerConnection connection;
		
		public HttpClientInfo(SocketChannel chennelTCp, ServerConnection connection) {
			this.chennelTCp = chennelTCp;
			this.connection = connection;
		}
		
		@Override
		public Channel getChannel() {
			return chennelTCp;
		}

		@Override
		public SocketAddress getclientAddress() {
			return null;
		}

		@Override
		public ServerConnection getConnection() {
			return connection;
		}
	}
	
	/*---------------------- DatabaseManagement Class ------------------------*/
	
	private interface SqlCommandHandler {
		public void execute(String sqlCommand) throws SQLException;
	}
	
	
	public class DatabaseManagement implements DatabaseManagementInterface {
		private Statement currentStatement;
		private List<Object> resultQueryList;
		private int columnIndex;
		private String columnName;
		private Object resultQuery;
		private Object newValueColumn;
		
		private final String databaseName;
		private final String url;
		private final String userName;
		private final String password;
		
		private static final String CREATE_DATABASE_IF_NOT_EXISTS = 
												"CREATE DATABASE IF NOT EXISTS ";
		private static final String USE = "USE ";
		private static final String DROP_TABLE = "DROP TABLE ";
		private static final String SELECT_ALL_FROM = "SELECT * FROM ";
		private static final String WHERE = " WHERE ";
		private static final String UPDATE = "UPDATE ";
		private static final String SET = " SET ";
		private static final String DELETE_FROM = "DELETE FROM ";
		private static final String INSERT_INTO_IOTEVENT = "INSERT  INTO IOTEvent ";
		private static final String VALUES = " VALUES ";
		private static final String EQUAL = "=";
		
		
		/*---------------------- ExecuteSqlCommand Objects -------------------*/
		
		private SqlCommandHandler createDatabaseObj = new SqlCommandHandler() {
			
			@Override
			public void execute(String sqlCommand) throws SQLException {
				currentStatement.execute(sqlCommand);
			}
		};
		
		private SqlCommandHandler executeCommandObj = new SqlCommandHandler() {
			
			@Override
			public void execute(String sqlCommand) throws SQLException {
				currentStatement.execute(USE + databaseName);
				currentStatement.execute(sqlCommand);
			}
		};
		
		private SqlCommandHandler createListOfOneRow = new SqlCommandHandler() {
			
			@Override
			public void execute(String sqlCommand) throws SQLException {
				ResultSet resultQuerySet = executeQuery(sqlCommand);
				resultQueryList = resultSetToArrayList(resultQuerySet);
				resultQuerySet.close();
			}
		};
		
		private SqlCommandHandler readFieldAccordingIndex = new SqlCommandHandler() {
			
			@Override
			public void execute(String sqlCommand) throws SQLException {
				ResultSet resultQuerySet = executeQuery(sqlCommand);
				resultQuerySet.next();
				resultQuery = resultQuerySet.getObject(columnIndex);
				resultQuerySet.close();
			}
		};
		
		private SqlCommandHandler readFieldAccordingName = new SqlCommandHandler() {
			
			@Override
			public void execute(String sqlCommand) throws SQLException {
				ResultSet resultQuerySet = executeQuery(sqlCommand);
				resultQuerySet.next();
				resultQuery = resultQuerySet.getObject(columnName);
				resultQuerySet.close();
			}
		};
		
		private SqlCommandHandler updateFieldAccordingIndex = new SqlCommandHandler() {
			
			@Override
			public void execute(String sqlCommand) throws SQLException {
				ResultSet resultQuerySet = executeQuery(sqlCommand);
				resultQuerySet.next();
				resultQuerySet.updateObject(columnIndex, newValueColumn);
				resultQuerySet.updateRow();
				resultQuerySet.close();
			}
		};
		
		/*--------------------------------------------------------------------*/
		

		public DatabaseManagement(String url, String userName, String password,
								String databaseName)  throws SQLException {
			Objects.requireNonNull(databaseName, url);
			Objects.requireNonNull(userName, password);
			

			this.url = url;
			this.userName = userName;
			this.password = password;
			this.databaseName = databaseName;
			
			String sqlCommand = CREATE_DATABASE_IF_NOT_EXISTS + databaseName;
			executeSqlCommand(sqlCommand, createDatabaseObj);
		}
		
		private ResultSet executeQuery(String sqlCommand) throws SQLException {
			currentStatement.execute(USE + databaseName);
			ResultSet resultQuerySet = currentStatement.executeQuery(sqlCommand);
			
			return resultQuerySet;
		}
		
		private void executeSqlCommand(String sqlCommand, 
								SqlCommandHandler sqlHandler) throws SQLException {
			try(
					java.sql.Connection connection = DriverManager.getConnection(url, 
												userName, password);
					Statement statement = connection.createStatement(
							ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			){
				this.currentStatement = statement;
				sqlHandler.execute(sqlCommand);
			}
		}
		
		public void createTable(String sqlCommand) throws SQLException {
			Objects.requireNonNull(sqlCommand);
			
			executeSqlCommand(sqlCommand, executeCommandObj);
		}
		
		public void deleteTable(String tableName) throws SQLException {
			Objects.requireNonNull(tableName);
			
			String sqlCommand =  DROP_TABLE + tableName;
			executeSqlCommand(sqlCommand, executeCommandObj);
		}
		
		public void createRow(String sqlCommand) throws SQLException {
			Objects.requireNonNull(sqlCommand);
			
			executeSqlCommand(sqlCommand, executeCommandObj);
		}
		
		/* rawData example: "serialNumber | description | timeStamp" */
		public void createIOTEvent(String rawData) throws SQLException { 
			Objects.requireNonNull(rawData);
			
			String[] valuesColumn = rawData.split("\\|");
			String sqlCommand = INSERT_INTO_IOTEVENT + VALUES + "(null,";
			
			for (int i = 0; i < valuesColumn.length; i++) {
				sqlCommand += valuesColumn[i];
				
				if(i + 1 == valuesColumn.length) {
					sqlCommand += " )";
				}
				else {
					sqlCommand += ", ";
				}
			}

			executeSqlCommand(sqlCommand, executeCommandObj);
		}
		
		public List<Object> readRow(String tableName, String columnName, 
								Object columnValue) throws SQLException {
			Objects.requireNonNull(tableName, columnName);
			Objects.requireNonNull(columnValue);
			
			String sqlCommand =  SELECT_ALL_FROM + tableName + WHERE + 
									columnName + EQUAL + "'" +  columnValue + "'";
			executeSqlCommand(sqlCommand, createListOfOneRow);
			
			return resultQueryList;
		}
		
		public Object readField(String tableName, String primaryKeyColumnName, 
						Object primaryKeyVal, int columnIndex) throws SQLException {
			Objects.requireNonNull(tableName, primaryKeyColumnName);
			Objects.requireNonNull(primaryKeyVal);
			
			String sqlCommand =  SELECT_ALL_FROM + tableName + WHERE + 
								primaryKeyColumnName + EQUAL + "'" + primaryKeyVal + "'";
			this.columnIndex = columnIndex;
			executeSqlCommand(sqlCommand, readFieldAccordingIndex);
			
			return resultQuery;
		}
		
		public Object readField(String tableName, String primaryKeyColumnName, 
					Object primaryKeyVal, String columnName) throws SQLException {
			Objects.requireNonNull(tableName, primaryKeyColumnName);
			Objects.requireNonNull(primaryKeyVal);
			
			String sqlCommand =  SELECT_ALL_FROM + tableName + WHERE + 
					primaryKeyColumnName + EQUAL + "'" + primaryKeyVal + "'";
			this.columnName = columnName;
			executeSqlCommand(sqlCommand, readFieldAccordingName);
			
			return resultQuery;
		}
		
		public void updateField(String tableName, String primaryKeyColumnName, 
							Object primaryKeyVal, int columnIndex, Object newValue) 
									throws SQLException {
			Objects.requireNonNull(tableName, primaryKeyColumnName);
			Objects.requireNonNull(primaryKeyVal);
			Objects.requireNonNull(newValue);
			
			this.columnIndex = columnIndex;
			this.newValueColumn = newValue;
			String sqlCommand =  SELECT_ALL_FROM + tableName + WHERE + 
								primaryKeyColumnName + EQUAL + primaryKeyVal;
			
			executeSqlCommand(sqlCommand, updateFieldAccordingIndex);
		}
		
		public void updateField(String tableName, String primaryKeyColumnName, 
						Object primaryKeyVal, String columnName, Object newValue) 
								throws SQLException {
			Objects.requireNonNull(tableName, primaryKeyColumnName);
			Objects.requireNonNull(primaryKeyVal);
			Objects.requireNonNull(newValue);
			
			String sqlCommand =  UPDATE + tableName + 
								SET + columnName + EQUAL + newValue +
								WHERE + primaryKeyColumnName + EQUAL + primaryKeyVal;
			
			executeSqlCommand(sqlCommand, executeCommandObj);
		}
		
		public void deleteRow(String tableName, String primaryKeyColumnName, 
								Object primaryKeyVal) throws SQLException {
			Objects.requireNonNull(tableName, primaryKeyColumnName);
			Objects.requireNonNull(primaryKeyVal);
			
			String sqlCommand = DELETE_FROM + tableName + WHERE + 
					primaryKeyColumnName + EQUAL + primaryKeyVal;

			executeSqlCommand(sqlCommand, executeCommandObj);
		}
		
		private List<Object> resultSetToArrayList(ResultSet resultSet) 
															throws SQLException {
			Objects.requireNonNull(resultSet);
			
			ResultSetMetaData metaData = (ResultSetMetaData) resultSet.getMetaData();
			int numOfColumns = metaData.getColumnCount();
			List<Object> list = new ArrayList<>(numOfColumns);

			while (resultSet.next()) {
				for (int i = 1; i <= numOfColumns; ++i) {
					list.add(resultSet.getObject(i));
				}
			}

			return list;
		}
	}
	
	
}