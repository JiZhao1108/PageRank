public class Driver {

    public static void main(String[] args) throws Exception {

        UnitMultiplication f1 = new UnitMultiplication();
        UnitSum f2 = new UnitSum();

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String subPR = args[2];
        float beta = Float.parseFloat(args[3]);
        int count = Integer.parseInt(args[4]);//convergence times

        for (int i = 0; i < count; i++) {
            String[] path1 = {transitionMatrix, prMatrix + i, subPR + i, String.valueOf(beta)};
            String[] path2 = {subPR + i, prMatrix + i, prMatrix + (i + 1), String.valueOf(beta)};

            f1.main(path1);
            f2.main(path2);
        }
    }
}
