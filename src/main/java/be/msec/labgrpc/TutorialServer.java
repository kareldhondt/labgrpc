package be.msec.labgrpc;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class TutorialServer {
	private static final Logger logger = Logger.getLogger(TutorialServer.class.getName());
	
	private final int port;
	private final Server server;
	
	static ArrayList<Calculation> history;
	
	public TutorialServer(int port) throws IOException {
		this(ServerBuilder.forPort(port), port);
	}
	
	public TutorialServer(ServerBuilder<?> serverBuilder, int port){
		this.port = port;
		if(history == null)
			history = new ArrayList<Calculation>();
		server = serverBuilder.addService(new CalculatorService()).build();
	}
	
	public void start() throws IOException{
		server.start();
		logger.info("Server started, listening on " + port);
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
	        public void run() {
	          // Use stderr here since the logger may has been reset by its JVM shutdown hook.
	          System.err.println("*** shutting down gRPC server since JVM is shutting down");
	          TutorialServer.this.stop();
	          System.err.println("*** server shut down");
	        }
	      });
	}

	  /** Stop serving requests and shutdown resources. */
	  public void stop() {
	    if (server != null) {
	      server.shutdown();
	    }
	  }

	  /**
	   * Await termination on the main thread since the grpc library uses daemon threads.
	   */
	  private void blockUntilShutdown() throws InterruptedException {
	    if (server != null) {
	      server.awaitTermination();
	    }
	  }

	public static void main(String[] args) throws Exception{
		TutorialServer server = new TutorialServer(50050);
	    server.start();
	    server.blockUntilShutdown();

	}
	
	private static class CalculatorService extends CalculatorGrpc.CalculatorImplBase {
		
		
		@Override
		public void calculateSum(Sum request, StreamObserver<CalculatorReply> responseObserver){
			responseObserver.onNext(sum(request));
			responseObserver.onCompleted();
		}
		
		@Override
		public StreamObserver<Sum> streamingSum(final StreamObserver<CalculatorReply> responseObserver){
			return new StreamObserver<Sum>() {
				int sumCount;
				CalculatorReply tempSolution;
				
				@Override
				public void onNext(Sum sum){
					if(sumCount == 0)
						tempSolution = sum(sum);
					else
						tempSolution = sum(sum, tempSolution);
						
					sumCount++;
				}
				
				@Override
				public void onError(Throwable t){
					logger.log(Level.WARNING, "Encountered error in StreamingSum", t);
				}
				
				@Override
				public void onCompleted() {
					responseObserver.onNext(tempSolution);
					responseObserver.onCompleted();
				}
			};
		}
		
		@Override
		public void calculatorHistory(Empty nul, StreamObserver<Calculation> responseObserver){
			for(int i = 0; i < history.size(); i++){
				responseObserver.onNext(history.get(i));
			}
			
			responseObserver.onCompleted();
		}
		
		private CalculatorReply sum(Sum request){
			CalculatorReply reply = CalculatorReply.newBuilder().setSolution(request.getA() + request.getB()).build();
			history.add(Calculation.newBuilder().setSum(request).setSolution(reply).setIndex(history.size()).build());
			
			return reply;
		}
		
		private CalculatorReply sum(Sum request, CalculatorReply extra){
			CalculatorReply reply;
			Sum newSum;
			
			reply = sum(request);
			newSum = Sum.newBuilder().setA(reply.getSolution()).setB(extra.getSolution()).build();
			
			reply = sum(newSum);
			
			return reply;
		}
	}

}
