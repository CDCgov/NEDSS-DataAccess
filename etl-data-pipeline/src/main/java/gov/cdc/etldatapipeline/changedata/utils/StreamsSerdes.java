package gov.cdc.etldatapipeline.changedata.utils;

import gov.cdc.etldatapipeline.changedata.model.NbsPage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

	public static Serde<NbsPage> NbsPageSerde() {
		return new NbsPageSerde();
	}

	public static final class NbsPageSerde extends WrapperSerde<NbsPage> {
		public NbsPageSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(NbsPage.class, false));
		}
	}
}
