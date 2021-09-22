package ecs;

public class ECSNode implements IECSNode{
    String nodeName;
    String nodeHost;
    int nodePort;
    public String[] nodeHashRange;
    public Process proc;

    public ECSNode(String nodeName, String nodeHost, int nodePort, String[] nodeHashRange){
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.nodeHashRange = nodeHashRange;
    }
    public String print(){
        return nodeName + " " + nodeHost + " " + nodePort + " " + nodeHashRange[0] + " " + nodeHashRange[1] + "\n";
    }

    @Override
    public String getNodeName(){
        return nodeName;
    }

    @Override
    public String getNodeHost(){
        return nodeHost;
    }

    @Override
    public int getNodePort(){
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange(){
        return nodeHashRange;
    }
}
