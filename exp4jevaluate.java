package spotify.heroic;

class Exp4jEvaluateFunction {

    /**
     * This is the code that is evaluated on every Point of the Series'. Basically
     * the this.tokens would be like ['x',2.4215,'/','y',63.232].
     */
    public double evaluate() {

        final ArrayStack output = new ArrayStack();

        for (int i = 0; i < tokens.length; i++) { // this might be like 5

            Token t = tokens[i];

            if (t.getType() == Token.TOKEN_NUMBER) { // nope

                output.push(((NumberToken) t).getValue());

            } else if (t.getType() == Token.TOKEN_VARIABLE) { // yep

                final String name = ((VariableToken) t).getName();
                final Double value = this.variables.get(name);
                if (value == null) {
                    throw new IllegalArgumentException("No value has been set for the setVariable '" + name + "'.");
                }
                output.push(value);

            } else if (t.getType() == Token.TOKEN_OPERATOR) {

                OperatorToken op = (OperatorToken) t;
                if (output.size() < op.getOperator().getNumOperands()) {
                    throw new IllegalArgumentException(
                            "Invalid number of operands available for '" + op.getOperator().getSymbol() + "' operator");
                }
                if (op.getOperator().getNumOperands() == 2) {
                    /* pop the operands and push the result of the operation */
                    double rightArg = output.pop();
                    double leftArg = output.pop();
                    output.push(op.getOperator().apply(leftArg, rightArg));
                } else if (op.getOperator().getNumOperands() == 1) {
                    /* pop the operand and push the result of the operation */
                    double arg = output.pop();
                    output.push(op.getOperator().apply(arg));
                }

            } else if (t.getType() == Token.TOKEN_FUNCTION) {
                FunctionToken func = (FunctionToken) t;
                final int numArguments = func.getFunction().getNumArguments();
                if (output.size() < numArguments) {
                    throw new IllegalArgumentException("Invalid number of arguments available for '"
                            + func.getFunction().getName() + "' function");
                }
                /* collect the arguments from the stack */
                double[] args = new double[numArguments];
                for (int j = numArguments - 1; j >= 0; j--) {
                    args[j] = output.pop();
                }
                output.push(func.getFunction().apply(args));

            }
        }

        if (output.size() > 1) {
            throw new IllegalArgumentException(
                    "Invalid number of items on the output queue. Might be caused by an invalid number of arguments for a function.");
        }

        return output.pop();
    }

}
