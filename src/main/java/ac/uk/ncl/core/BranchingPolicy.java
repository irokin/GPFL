package ac.uk.ncl.core;

import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

import java.util.LinkedList;
import java.util.Queue;

public class BranchingPolicy {
    public static BranchOrderingPolicy
    PreorderBFS() {
        return (startBranch, expander) -> new BranchSelector() {
            private final Queue<TraversalBranch> queue = new LinkedList<>();
            private TraversalBranch current = startBranch;

            @Override
            public TraversalBranch next( TraversalContext metadata )
            {
                TraversalBranch result = null;
                while ( result == null )
                {
                    TraversalBranch next = current.next( expander, metadata );
                    if ( next != null )
                    {
                        queue.add( next );
                        result = next;
                    }
                    else
                    {
                        current = queue.poll();
                        if ( current == null )
                        {
                            return null;
                        }
                    }
                }
                return result;
            }
        };
    }
}
