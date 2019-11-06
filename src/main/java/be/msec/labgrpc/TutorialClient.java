package be.msec.labgrpc;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.StreamObservers;

import be.msec.labgrpc.CalculatorGrpc.CalculatorBlockingStub;
import be.msec.labgrpc.CalculatorGrpc.CalculatorStub;

public class TutorialClient {
	private static final Logger logger = Logger.getLogger(TutorialClient.class.getName());
	
	private final ManagedChannel channel;
	private final CalculatorBlockingStub blockingStub;
	private final CalculatorStub asyncStub;
	
	public TutorialClient(String host, int port){
		this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
	}
	
	public TutorialClient(ManagedChannelBuilder<?> channelBuilder) {
		channel = channelBuilder.build();
		blockingStub = CalculatorGrpc.newBlockingStub(channel);
		asyncStub = CalculatorGrpc.newStub(channel);
	}

	public void shutdown() throws InterruptedException {
		channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}
	
	public void calculateSum(int a, int b){
		info("Calculating sum of {0} and {1}", a, b);
		
		Sum request = Sum.newBuilder().setA(a).setB(b).build();
		CalculatorReply reply;
		try{
			reply = blockingStub.calculateSum(request);
		} catch (StatusRuntimeException e) {
			logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
			return;
		}
		
		info("Solution of the {0} + {1} = {2}", a, b, reply.getSolution());
	}
	
	public void streamingSum(int operands) throws InterruptedException {
		info("Streaming sum");
		final CountDownLatch finishLatch = new CountDownLatch(1);
		StreamObserver<CalculatorReply> responseObserver = new StreamObserver<CalculatorReply>() {
			@Override
			public void onNext(CalculatorReply reply) {
				info("Finished streaming sum. The solution is {0}", reply.getSolution());
			}
			
			@Override
			public void onError(Throwable t){
				Status status = Status.fromThrowable(t);
				logger.log(Level.WARNING, "StreamingSum failed:{0}", status);
				finishLatch.countDown();
			}
			
			@Override
			public void onCompleted(){
				info("Finished StreamingSum");
				finishLatch.countDown();
			}
		};
		
		StreamObserver<Sum> requestObserver = asyncStub.streamingSum(responseObserver);
		try{
			int a;
			int b;
			Sum request;
			for(int i = 0; i < (operands+1)/2; i++){
				a = (int)(Math.random() * 10);
				b = (int)(Math.random() * 10);
				if(i == ((operands+1)/2-1) && (operands % 2) != 0){
					b = 0;
					info("Adding new summand {0}", a);
				} else
					info("Adding new summands {0} and {1} ", a, b);
				
				request = Sum.newBuilder().setA(a).setB(b).build();
				
				requestObserver.onNext(request);
				
				Thread.sleep(1000);
				if (finishLatch.getCount() == 0){
					// RPC completed or errored before we finished sending.
					// Sending further request won't error, but they will just be thrown away.
					return;
				}
					
			}
		} catch (RuntimeException e){
			requestObserver.onError(e);
			throw e;
		}
		
		// Mark the end of requests
		requestObserver.onCompleted();
		
		// Receiving happens asynchronously
		finishLatch.await(1, TimeUnit.MINUTES);
	}
	
	public void calculatorHistory(){
		info("Requesting calculator history");
		
		Iterator<Calculation> calculations;
		try{
			calculations = blockingStub.calculatorHistory(Empty.newBuilder().build());
		} catch (StatusRuntimeException e) {
			logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
			return;
		}
		
		StringBuilder responseLog = new StringBuilder("History:\n");
		while(calculations.hasNext()){
			Calculation calculation = calculations.next();
			responseLog.append(calculation);
		}
		
		info(responseLog.toString());
	}

	public static void main(String[] args) throws InterruptedException {
		TutorialClient client = new TutorialClient("localhost", 50050);
		try{
			client.calculateSum(10, 5);
			
			client.streamingSum(5);
			
			client.calculatorHistory();
		} finally {
			client.shutdown();
		}
		

	}

	private static void info(String msg, Object... params) {
		logger.log(Level.INFO, msg, params);
	}

}
