package uk.ac.ncl.core;

public class RuleGeneration {
//    public static Set<Rule> progressivePathSampler(GraphDatabaseService graph, List<Instance> train) {
//        Set<Rule> abstractRules = new HashSet<>();
//
//        try(Transaction tx = graph.beginTx()) {
//            Set<Rule> closedRules = new HashSet<>();
//            Set<Rule> currentClosedRules = new HashSet<>();
//            Set<Rule> openRules = new HashSet<>();
//            Set<Rule> currentOpenRules = new HashSet<>();
//            Random rand = new Random();
//            Timer timer = new Timer(1, 20);
//
//            int visitedPaths = 0;
//            int visitedInstances = 0;
//            int closedCurrentDepth = 1;
//            int maxClosedDepth = 3;
//            int openCurrentDepth = 1;
//            int maxOpenDepth = 3;
//            int closedPathCounter = 0;
//            int openPathCounter = 0;
//            double saturation = 0.99;
//            boolean checkClosed = true;
//
//            timer.start();
//            do {
//                visitedInstances++;
//                Instance instance = train.get(rand.nextInt(train.size()));
//
//                if(checkClosed && closedCurrentDepth <= maxClosedDepth) {
//                    Set<Path> localPaths = new HashSet<>(GraphOps.pathSamplingTraversal(graph, instance, closedCurrentDepth, 10));
//                    visitedPaths += localPaths.size();
//                    closedPathCounter += localPaths.size();
//                    localPaths.forEach( path -> {
//                        Rule abstractRule = GenOps.abstraction(path, instance);
//                        if(abstractRule.isClosed()) currentClosedRules.add(abstractRule);
//                    });
//                    if(timer.tick()) {
//                        checkClosed = false;
//                        int overlaps = 0;
//                        for (Rule currentClosedRule : currentClosedRules) if(closedRules.contains(currentClosedRule)) overlaps++;
//                        closedRules.addAll(currentClosedRules);
//
//                        System.out.println("# Mine Closed Rules - Tick " + timer.getTickCounts()
//                                + "\n# Sampled Paths: " + closedPathCounter
//                                + "\n# New Rules: " + (currentClosedRules.size() - overlaps)
//                                + "\n# Total Rules: " + closedRules.size()
//                                + "\n");
//                        closedPathCounter = 0;
//
//                        if((double) overlaps / currentClosedRules.size() > saturation || currentClosedRules.size() == 0) {
//                            System.out.println("# Closed Progressed to: " + ++closedCurrentDepth + "\n");
//                        }
//                    }
//                } else if(openCurrentDepth <= maxOpenDepth) {
//                    Set<Path> localPaths = new HashSet<>(GraphOps.pathSamplingTraversal(graph, instance, openCurrentDepth, 10));
//                    visitedPaths += localPaths.size();
//                    openPathCounter += localPaths.size();
//
//                    localPaths.forEach( path -> {
//                        Rule abstractRule = GenOps.abstraction(path, instance);
//                        if(!abstractRule.isClosed()) {
//                            InstantiatedRule headRule = new InstantiatedRule(abstractRule, instance, path, 0);
//                            InstantiatedRule bothRule = new InstantiatedRule(abstractRule, instance, path, 2);
//                            currentOpenRules.add(abstractRule);
//                            currentOpenRules.add(headRule);
//                            currentOpenRules.add(bothRule);
//                            GenOps.deHierarchy.put(abstractRule, headRule);
//                            GenOps.deHierarchy.put(abstractRule, bothRule);
//                            abstractRules.add(abstractRule);
//                        }
//                    });
//                    if(timer.tick()) {
//                        checkClosed = true;
//                        int overlaps = 0;
//                        for (Rule currentOpenRule : currentOpenRules) if(openRules.contains(currentOpenRule)) overlaps++;
//                        openRules.addAll(currentOpenRules);
//
//                        System.out.println("# Mine Open Rules - Tick " + timer.getTickCounts()
//                                + "\n# Sampled Paths: " + openPathCounter
//                                + "\n# New Rules: " + (currentOpenRules.size() - overlaps)
//                                + "\n# Total Rules: " + openRules.size()
//                                + "\n");
//
//                        openPathCounter = 0;
//                        if((double) overlaps / currentOpenRules.size() > saturation) {
//                            System.out.println("Open Progressed to: " + ++openCurrentDepth);
//                        }
//                    }
//                } else break;
//            } while (timer.continues());
//
//            abstractRules.addAll(closedRules);
//            System.out.println("Paths: " + visitedPaths);
//            System.out.println("Instances: " + visitedInstances);
//            System.out.println("Closed Rules: " + closedRules.size());
//            System.out.println("Open Rules: " + openRules.size());
//            System.out.println("Open Depth: " + openCurrentDepth);
//            System.out.println("Closed Depth: " + closedCurrentDepth);
//
//            tx.success();
//        }
//        return abstractRules;
//    }
}
