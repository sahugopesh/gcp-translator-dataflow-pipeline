package efx.sup.translator.pipeline.http.client;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import java.io.File;

import javax.net.ssl.SSLException;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContext;

public class HttpClient {
    AsyncHttpClient httpClient;
    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    
    public HttpClient() throws SSLException {
        DefaultAsyncHttpClientConfig.Builder configBuilder = config()
                .setConnectTimeout(90000)
                .setRequestTimeout(90000)
                .setMaxRequestRetry(3)
                .setMaxConnectionsPerHost(100)
                .setReadTimeout(90000);
        
        ThreadPoolTaskExecutor threadFactory = new ThreadPoolTaskExecutor();
        threadFactory.setMaxPoolSize(32);
        threadFactory.setKeepAliveSeconds(300);
        /**
        SslContext sslContext = null;
        File certChainFile = new File("certificate.pem");
        sslContext = SslContext.newClientContext(certChainFile);
        
        configBuilder.setSslContext(sslContext);
        */
        configBuilder.setThreadFactory(threadFactory);

        AsyncHttpClientConfig clientConfig = configBuilder.build();
        httpClient = asyncHttpClient(clientConfig);
    }

    public void send(String input) throws JsonProcessingException{
    	
    	//BodyGenerator bgBodyGenerator = new ByteArrayBodyGenerator("test");

        logger.info("In Event Transformer.");
        ObjectNode objectNode = mapper.createObjectNode();
        //objectNode.put("source","httpevent");
        objectNode.put("billingData", input.split(":")[0].trim());
        objectNode.put("invoicePrice", input.split(":")[1].trim());

        String event = mapper.writeValueAsString(objectNode);
        
        System.out.println("event ::"+event);
        Request request = new RequestBuilder()
                //.setUrl("https://reqbin.com/sample/post/json")
                .setUrl("http://localhost:8090/sup/api/v1/data")
        		.setMethod(HttpMethod.POST.name())
                //.setHeader("Authorization", "XXXX")
        		.setHeader("Content-Type", "application/json")
                .setBody(event)
        		//.setBody(bgBodyGenerator)
                .build();
        try {
        ListenableFuture<Response> responseListenableFuture = httpClient.executeRequest(request, new AsyncCompletionHandler<Response>() {
            @Override
            public Response onCompleted(Response response) throws Exception {
                System.out.println("Respose code from SUP API :: " + response.getStatusCode());
                return null;
            }
        });
        }
        catch (Exception e) {
			// TODO: handle exception
        	System.out.println(e);
		}
        
        
    }
}
