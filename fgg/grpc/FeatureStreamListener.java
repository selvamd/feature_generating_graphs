package fgg.grpc;
import java.util.*;
import fgg.utils.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

//Single listener per stream or thread
public abstract class FeatureStreamListener
{
	private StreamObserver<FggDataServiceOuterClass.FggData> receiver = createReceiver(this);
	private List<FeatureData> list = new ArrayList<FeatureData>();
    private boolean done = false;

	public abstract void onNext(FeatureData data);
	public abstract void onComplete(List<FeatureData> data);

    //Non-blocking get
    public List<FeatureData> get() { return (isComplete())? list:null; }
    public boolean isComplete() { return (done)? !(done=false):false; }

	public StreamObserver<FggDataServiceOuterClass.FggData> getReceiver() { return receiver; }

	private StreamObserver<FggDataServiceOuterClass.FggData> createReceiver(final FeatureStreamListener obs)
	{
		return new StreamObserver<FggDataServiceOuterClass.FggData>() {
			FeatureData feature = new FeatureData();
			public void onNext(FggDataServiceOuterClass.FggData data)
			{
				if (feature.add(data))
				{
					if (feature.isFullXmit())
                    {
						obs.onNext(feature);
                        obs.list.add(feature);
                    }
					feature = new FeatureData();
				}
			}

			public void onError(Throwable t) {
				t.printStackTrace();
                obs.onComplete(obs.list);
                obs.list.clear();
                obs.done = true;
			}

			public void onCompleted() {
                obs.onComplete(obs.list);
                obs.list.clear();
                obs.done = true;
			}
      };
	}
}
