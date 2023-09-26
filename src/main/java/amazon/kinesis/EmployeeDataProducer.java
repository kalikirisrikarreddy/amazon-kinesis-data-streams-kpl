package amazon.kinesis;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Executors;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.javafaker.Faker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class EmployeeDataProducer {

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	public static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writerFor(Employee.class);
	private static final String KINESIS_DATA_STREAM_NAME = "employee-data-stream";

	public static void main(String[] args) throws JsonProcessingException, InterruptedException {
		KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration();
		kinesisProducerConfiguration.setRecordMaxBufferedTime(5000L);
		kinesisProducerConfiguration.setMaxConnections(Runtime.getRuntime().availableProcessors());
		kinesisProducerConfiguration.setRequestTimeout(30000L);

		KinesisProducer kinesisProducer = new KinesisProducer(kinesisProducerConfiguration);
		FutureCallback<UserRecordResult> myCallback = new FutureCallback<UserRecordResult>() {
			@Override
			public void onFailure(Throwable t) {
				t.printStackTrace();
			};

			@Override
			public void onSuccess(UserRecordResult result) {
				System.out.println("Record with sequence number " + result.getSequenceNumber() + " written to "
						+ result.getShardId() + " shard. ");
			};
		};
		Faker faker = new Faker();
		for (int i = 0; i < 100; ++i) {
			String uuid = UUID.randomUUID().toString();
			ByteBuffer data = ByteBuffer.wrap(OBJECT_WRITER.writeValueAsBytes(
					new Employee(uuid, faker.name().fullName(), faker.company().profession(), faker.company().name())));
			ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(KINESIS_DATA_STREAM_NAME, uuid, data);
			System.out.println("produced " + (i + 1) + "th record");
			Futures.addCallback(f, myCallback, Executors.newCachedThreadPool());
		}
		Thread.sleep(30000L);
	}
}
