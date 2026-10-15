"""Безопасный вычислитель простых арифметических выражений без eval()."""
import ast
import operator

OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
UNARY = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def calculate(expression: str) -> str:
    if not expression or len(expression) > 200:
        return "ОШИБКА: выражение пустое или слишком длинное"
    try:
        tree = ast.parse(expression, mode="eval")

        def visit(node, depth=0):
            if depth > 20:
                raise ValueError("слишком сложное выражение")
            if isinstance(node, ast.Constant) and type(node.value) in (int, float):
                value = node.value
            elif isinstance(node, ast.BinOp) and type(node.op) in OPERATORS:
                left = visit(node.left, depth + 1)
                right = visit(node.right, depth + 1)
                if isinstance(node.op, ast.Pow) and (abs(right) > 10 or abs(left) > 1_000_000):
                    raise ValueError("слишком большая степень")
                value = OPERATORS[type(node.op)](left, right)
            elif isinstance(node, ast.UnaryOp) and type(node.op) in UNARY:
                value = UNARY[type(node.op)](visit(node.operand, depth + 1))
            else:
                raise ValueError("разрешены только числа и арифметические операции")
            if isinstance(value, complex) or abs(value) > 1e15:
                raise ValueError("слишком большое число")
            return value

        return str(visit(tree.body))
    except (SyntaxError, ValueError, ZeroDivisionError, OverflowError, TypeError) as error:
        return f"ОШИБКА: {error}"
