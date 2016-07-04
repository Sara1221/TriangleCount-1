package Driver;

import CountNum.CountNum;
import GetEdge.GetEdge;
import GraphBuilder.GraphBuilder;

import java.io.IOException;

/**
 * Created by hadoop on 16-5-29.
 */
public class Driver {

    public static void main(String[]args) throws Exception {
        String[] getEdge={args[0],args[1]+"/temp1"};
        GetEdge.main(getEdge);

        String[]graphBuilder={args[1]+"/temp1",args[1]+"/temp2"};
        GraphBuilder.main(graphBuilder);

        String[]countNum={args[1]+"/temp2",args[1]+"/final"};
        CountNum.main(countNum);
    }
}
