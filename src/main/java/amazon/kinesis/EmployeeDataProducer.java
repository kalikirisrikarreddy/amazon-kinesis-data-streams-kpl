package amazon.kinesis;

import static org.apache.commons.lang3.StringUtils.isBlank;

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
	private static final String N_STR = System.getenv("N");
	private static final int N = isBlank(N_STR) ? 1000 : Integer.parseInt(N_STR);

	public static void main(String[] args) throws JsonProcessingException, InterruptedException {
		KinesisProducerConfiguration kpc = new KinesisProducerConfiguration();
		kpc.setRecordMaxBufferedTime(60000L);
		kpc.setRequestTimeout(60000L);
		kpc.setRecordTtl(Integer.MAX_VALUE);

		KinesisProducer kinesisProducer = new KinesisProducer(kpc);
		FutureCallback<UserRecordResult> myCallback = new FutureCallback<>() {
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
		for (int i = 0; i < N; ++i) {
			String uuid = UUID.randomUUID().toString();
			ByteBuffer data = ByteBuffer.wrap(OBJECT_WRITER.writeValueAsBytes(
					new Employee(uuid, faker.name().fullName(), faker.company().profession(), faker.company().name())));
			ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(KINESIS_DATA_STREAM_NAME, uuid, data);
			System.out.println("produced " + (i + 1) + "th record");
			Futures.addCallback(f, myCallback, Executors.newCachedThreadPool());
		}
		Thread.sleep(600 * 1000L);
	}
}
