package efx.sup.translator.pipeline.http.client;

import javax.net.ssl.SSLException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * A DoFn to write to Http.
 */
public class HttpWriter<T> extends PTransform<PCollection<String>, PDone> {

    private static final Logger logger = LoggerFactory.getLogger(HttpWriter.class);


    @Override
    public PDone expand(PCollection<String> input) {
        input.apply(ParDo.of(new Fn<>()));
        return PDone.in(input.getPipeline());
    }

    private static class Fn<T> extends DoFn<String, String> {

        private HttpClient asyncHttpClient;

        private Fn() {
        }

        @Setup
        public void onSetup() {
            try {
				this.asyncHttpClient = new HttpClient();
			} catch (SSLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        @ProcessElement
        public void onElement(final ProcessContext context) throws JsonProcessingException {
            final String element = context.element();
            //logger.info("The input in writer: "+element);
            System.out.println("The input in writer: "+element);
			this.asyncHttpClient.send(element);

        }
    }
}

