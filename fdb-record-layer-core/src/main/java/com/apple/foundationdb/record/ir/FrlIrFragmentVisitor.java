/*
 * FrlIrFragmentVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.ir;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IAssignment;
import com.apple.foundationdb.record.ir.fragments.IBlock;
import com.apple.foundationdb.record.ir.fragments.IClosure;
import com.apple.foundationdb.record.ir.fragments.IEnumType;
import com.apple.foundationdb.record.ir.fragments.IExpression;
import com.apple.foundationdb.record.ir.fragments.IFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionCall;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclaration;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinition;
import com.apple.foundationdb.record.ir.fragments.IFunctionReference;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.ILiteral;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IProgram;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStatement;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.IVariableReference;
import com.apple.foundationdb.record.ir.fragments.IVectorType;
import com.apple.foundationdb.record.ir.fragments.ParserHelpers;
import com.apple.foundationdb.record.ir.generated.FrlIrParser;
import com.apple.foundationdb.record.ir.generated.FrlIrParserBaseVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builds the fragments of an FRL-IR program from its parse tree: {@link #visitProgram} builds the {@link IProgram} of a
 * program, and the visit methods for types build the {@link IType} that a type denotes.
 *
 * <p>The visitor keeps the {@link Scope} of the construct it is in. A function definition declares its type parameters
 * and its parameters in a scope of their own, a block opens a scope for what its statements declare, and leaving a
 * construct returns to the enclosing scope. A name in a type is a type only if it is a type parameter in scope, so a
 * misspelled type is an error. A visitor holds the scope of one traversal, so each parse uses a new visitor.</p>
 *
 * <p>Statements are typed as they are built. The visitor first infers a statement: it declares what the statement
 * declares, and it adds the statement's equations to the {@code TypeInference} of the statement's scope. It then
 * solves the inference and builds the fragments from the solved types. The statements of a closure join the inference
 * of the statement that the closure is in, so they are built only once that statement is solved; every other
 * statement, including every statement of a function body, is solved and built on its own. A statement and an
 * expression thus produce something to build later rather than a fragment, so they are visited by visitors of their
 * own, one method for each alternative of the grammar.</p>
 */
@API(API.Status.EXPERIMENTAL)
public class FrlIrFragmentVisitor extends FrlIrParserBaseVisitor<IFragment> {

    @Nonnull
    private Scope scope = Scope.root();

    //
    // Programs and statements
    //

    @Nonnull
    @Override
    public IProgram visitProgram(@Nonnull final FrlIrParser.ProgramContext ctx) {
        return new IProgram(statements(ctx.statements(), null).builder().get());
    }

    /**
     * Infers the statements of a program, a block, or a closure. If the last statement is an expression, the expected
     * type of the value of the statements is its context, and its type is the type of the value.
     */
    @Nonnull
    private PendingStatements statements(@Nonnull final FrlIrParser.StatementsContext ctx,
                                         @Nullable final TypeInference.Term expectedValue) {
        final List<FrlIrParser.StatementContext> statementContexts = ctx.statement();
        final ImmutableList.Builder<Supplier<IStatement>> builders = ImmutableList.builder();
        TypeInference.Term valueType = null;
        if (!statementContexts.isEmpty()) {
            final int last = statementContexts.size() - 1;
            for (final FrlIrParser.StatementContext statementContext : statementContexts.subList(0, last)) {
                builders.add(statement(statementContext, null).builder());
            }
            final PendingStatement lastStatement = statement(statementContexts.get(last), expectedValue);
            builders.add(lastStatement.builder());
            valueType = lastStatement.valueType();
        }
        final ImmutableList<Supplier<IStatement>> pending = builders.build();
        return new PendingStatements(() -> pending.stream()
                .map(Supplier::get)
                .collect(ImmutableList.toImmutableList()), valueType);
    }

    /**
     * Infers a statement. A statement of a closure joins the inference of the statement that the closure is in, which
     * its scope carries. Any other statement gets a scope with an inference of its own, and it is inferred, solved, and
     * built right away.
     */
    @Nonnull
    private PendingStatement statement(@Nonnull final FrlIrParser.StatementContext ctx,
                                       @Nullable final TypeInference.Term expectedValue) {
        if (scope.inference() != null) {
            return new StatementInferenceVisitor(expectedValue).visit(ctx);
        }
        final TypeInference statementInference = new TypeInference();
        return inScope(scope.enterStatement(statementInference), () -> {
            final PendingStatement pending = new StatementInferenceVisitor(expectedValue).visit(ctx);
            statementInference.solve();
            final IStatement statement = pending.builder().get();
            return new PendingStatement(() -> statement, pending.valueType());
        });
    }

    /**
     * Builds a function definition. Its type parameters and its parameters are declared in the scope of the function,
     * which its body is nested in. The function is declared in the enclosing scope as soon as its signature is known,
     * before its body is built, so that the body and the statements after the definition can call it. The scope of the
     * function has no inference, so the statements of the body are typed one by one, also inside a closure, and the
     * type parameters of the function stay inside it.
     */
    @Nonnull
    private IFunctionDefinition functionDefinition(@Nonnull final FrlIrParser.FunctionDefinitionContext ctx) {
        final String name = name(ctx.identifier());
        final ImmutableList<IGenericType> typeParameters = typeParameters(ctx.typeParameters());
        final Scope enclosingScope = scope;
        return inScope(scope.enterFunction(typeParameters), () -> {
            final ImmutableList<IFunctionDefinition.Parameter> parameters = parameters(ctx.parameter());
            final IType resultType = ctx.type() == null ? voidType() : type(ctx.type());
            enclosingScope.declareFunction(name, typeParameters, functionType(parameters, resultType));
            parameters.forEach(parameter -> scope.declareVariable(parameter.name(), TypeInference.known(parameter.type())));
            final PendingStatements body = inScope(scope.enterBlock(), () -> statements(ctx.block().statements(),
                    resultType.isVoid() ? null : TypeInference.known(resultType)));
            return new IFunctionDefinition(name, typeParameters, parameters, resultType, new IBlock(body.builder().get()));
        });
    }

    /**
     * Builds a function declaration and declares the function in the enclosing scope.
     */
    @Nonnull
    private IFunctionDeclaration functionDeclaration(@Nonnull final FrlIrParser.FunctionDeclarationContext ctx) {
        final String name = name(ctx.identifier());
        final ImmutableList<IGenericType> typeParameters = typeParameters(ctx.typeParameters());
        final Scope enclosingScope = scope;
        return inScope(scope.enterFunction(typeParameters), () -> {
            final ImmutableList<IFunctionDefinition.Parameter> parameters = parameters(ctx.parameter());
            final IType resultType = ctx.type() == null ? voidType() : type(ctx.type());
            final IFunctionDeclaration declaration =
                    new IFunctionDeclaration(name, typeParameters, parameters, resultType);
            enclosingScope.declareFunction(name, typeParameters, functionType(parameters, resultType));
            return declaration;
        });
    }

    @Nonnull
    @Override
    public IFunctionDefinition.Parameter visitParameter(@Nonnull final FrlIrParser.ParameterContext ctx) {
        return new IFunctionDefinition.Parameter(name(ctx.identifier()), type(ctx.type()));
    }

    @Nonnull
    private ImmutableList<IGenericType> typeParameters(@Nullable final FrlIrParser.TypeParametersContext ctx) {
        return ctx == null
                ? ImmutableList.of()
                : ctx.identifier().stream()
                        .map(identifier -> new IGenericType(name(identifier), false))
                        .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private ImmutableList<IFunctionDefinition.Parameter> parameters(
            @Nonnull final List<FrlIrParser.ParameterContext> ctx) {
        return ctx.stream().map(this::visitParameter).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static IFunctionType functionType(@Nonnull final List<IFunctionDefinition.Parameter> parameters,
                                              @Nonnull final IType resultType) {
        return new IFunctionType(false, parameters.stream()
                .map(IFunctionDefinition.Parameter::type)
                .collect(ImmutableList.toImmutableList()), resultType);
    }

    //
    // Expressions
    //

    /**
     * Infers the type of an expression, a primary expression followed by calls, each of which calls the result of the
     * one before it. The expected type, if there is one, is the context of the expression: the type stated for a
     * variable, the type of a parameter, or the result type of a body.
     *
     * @param mismatch the message of the exception if the expression cannot have the expected type
     */
    @Nonnull
    private Inferred infer(@Nonnull final FrlIrParser.ExpressionContext ctx,
                           @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final List<FrlIrParser.CallSuffixContext> callSuffixes = ctx.callSuffix();
        if (callSuffixes.isEmpty()) {
            return new ExpressionInferenceVisitor(expected, mismatch).visit(ctx.primaryExpression());
        }
        // a callee has no context of its own: its type follows from the arguments and the context of the call
        Inferred expression = new ExpressionInferenceVisitor(null, mismatch).visit(ctx.primaryExpression());
        final int last = callSuffixes.size() - 1;
        for (final FrlIrParser.CallSuffixContext callSuffix : callSuffixes.subList(0, last)) {
            expression = inferCall(expression, callSuffix, null, mismatch);
        }
        return inferCall(expression, callSuffixes.get(last), expected, mismatch);
    }

    /**
     * Infers the type of a call: the callee must be a function of the types of the arguments, and the result type of
     * that function is the type of the call. The callee, and the expected type of the call, are unified with the
     * function type before the arguments are inferred, so that a closure among the arguments has the type of its
     * parameter as its context.
     */
    @Nonnull
    private Inferred inferCall(@Nonnull final Inferred callee, @Nonnull final FrlIrParser.CallSuffixContext ctx,
                               @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final TypeInference inference = inference();
        final String argumentMismatch =
                "a call must call a function that is not nullable with arguments of the types of its parameters";
        final ImmutableList<TypeInference.Term> parameterTypes = ctx.expression()
                .stream()
                .map(argument -> inference.unknown())
                .collect(ImmutableList.toImmutableList());
        final TypeInference.Term resultType = inference.unknown();
        inference.unify(callee.type(), inference.function(parameterTypes, resultType), argumentMismatch);
        if (expected != null) {
            inference.unify(resultType, expected, mismatch);
        }
        final ImmutableList.Builder<Inferred> arguments = ImmutableList.builder();
        for (int i = 0; i < parameterTypes.size(); i++) {
            arguments.add(infer(ctx.expression(i), parameterTypes.get(i), argumentMismatch));
        }
        final ImmutableList<Inferred> inferredArguments = arguments.build();
        return new Inferred(resultType, () -> new IFunctionCall(callee.builder().get(), inferredArguments.stream()
                .map(argument -> argument.builder().get())
                .collect(ImmutableList.toImmutableList())));
    }

    /**
     * Returns the type of a literal: a new inference variable that only a type the literal can denote can satisfy. Its
     * value is created for that type once the type is known.
     */
    @Nonnull
    private Inferred literal(@Nonnull final TypeInference.LiteralKind kind,
                             @Nonnull final Function<IType, Object> valueOfType) {
        final TypeInference.Term type = inference().literal(kind);
        return new Inferred(type, () -> {
            final IType resolvedType = TypeInference.resolve(type);
            return new ILiteral(valueOfType.apply(resolvedType), resolvedType);
        });
    }

    /**
     * Returns the value of a numeric literal as a value of the type that the literal has.
     */
    @Nonnull
    private static Object numericValue(@Nonnull final String text, @Nonnull final IType type) {
        // the constraint of a numeric literal only allows numeric primitive types
        final IPrimitiveType.Kind kind = ((IPrimitiveType)type).kind();
        final Object value;
        try {
            value = switch (kind) {
                case INT -> Integer.valueOf(text);
                case BIGINT -> Long.valueOf(text);
                // decimal to float to half rounds twice, which can differ from rounding once at a rounding midpoint
                case HALF -> Half.valueOf(Float.parseFloat(text));
                case FLOAT -> Float.valueOf(text);
                case DOUBLE -> Double.valueOf(text);
                default -> throw new IllegalStateException("a numeric literal must have a numeric type");
            };
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("a literal must be in the range of its type", e);
        }
        Preconditions.checkArgument(!isInfinite(value), "a literal must be in the range of its type");
        return value;
    }

    private static boolean isInfinite(@Nonnull final Object value) {
        return (value instanceof Half half && half.isInfinite())
                || (value instanceof Float floatValue && floatValue.isInfinite())
                || (value instanceof Double doubleValue && doubleValue.isInfinite());
    }

    /**
     * Returns the string that a string literal denotes: the text between the quotes, with every escape sequence
     * replaced by the character it stands for. The lexer guarantees the form of the escape sequences.
     */
    @Nonnull
    private static String stringValue(@Nonnull final String text) {
        final StringBuilder value = new StringBuilder();
        int i = 1;
        while (i < text.length() - 1) {
            final char c = text.charAt(i);
            if (c != '\\') {
                value.append(c);
                i++;
            } else if (text.charAt(i + 1) == 'u') {
                final int end = text.indexOf('}', i);
                value.appendCodePoint(unicodeScalar(text.substring(i + 3, end)));
                i = end + 1;
            } else {
                value.append(escapedCharacter(text.charAt(i + 1)));
                i += 2;
            }
        }
        return value.toString();
    }

    private static char escapedCharacter(final char escaped) {
        return switch (escaped) {
            case '0' -> '\0';
            case 't' -> '\t';
            case 'n' -> '\n';
            case 'r' -> '\r';
            // the lexer leaves \\, \", and \', which stand for the character after the backslash
            default -> escaped;
        };
    }

    /**
     * Returns the code point that the hexadecimal digits of a Unicode escape sequence denote, the digits between the
     * braces after the backslash and the {@code u}. As in Swift, there are one to eight digits, and the code point is a
     * Unicode scalar value: a code point that is not a surrogate.
     */
    private static int unicodeScalar(@Nonnull final String digits) {
        final long codePoint = digits.length() <= 8 ? Long.parseLong(digits, 16) : -1L;
        Preconditions.checkArgument(codePoint >= 0 && codePoint <= Character.MAX_CODE_POINT
                        && (codePoint < Character.MIN_SURROGATE || codePoint > Character.MAX_SURROGATE),
                "an escape sequence \\u{...} must have one to eight hexadecimal digits of a Unicode scalar value");
        return (int)codePoint;
    }

    //
    // Types
    //

    /**
     * Creates the type without its trailing {@code ?} and then applies it, so the rules for the individual types need
     * not know whether the type is nullable. Only a parenthesized type can already be nullable at this point, as in
     * {@code (int?)?}; nullability is a property of a type rather than a wrapper around it, so that is rejected.
     */
    @Nonnull
    @Override
    public IType visitValueType(@Nonnull final FrlIrParser.ValueTypeContext ctx) {
        final IType type = (IType)visit(ctx.baseType());
        if (ctx.QUESTION() == null) {
            return type;
        }
        Preconditions.checkArgument(!type.isNullable(), "a type can be made nullable only once");
        return type.withNullability(true);
    }

    @Nonnull
    @Override
    public IType visitParenthesizedType(@Nonnull final FrlIrParser.ParenthesizedTypeContext ctx) {
        return type(ctx.type());
    }

    @Nonnull
    @Override
    public IPrimitiveType visitUnitType(@Nonnull final FrlIrParser.UnitTypeContext ctx) {
        return voidType();
    }

    /**
     * Resolves a type parameter to the one that the nearest enclosing function declares by that name. Its nullability
     * is applied by the {@code valueType} around it.
     */
    @Nonnull
    @Override
    public IGenericType visitGenericType(@Nonnull final FrlIrParser.GenericTypeContext ctx) {
        return scope.lookupTypeParameter(name(ctx.identifier()))
                .orElseThrow(() -> new IllegalArgumentException("unknown type"));
    }

    @Nonnull
    @Override
    public IPrimitiveType visitBooleanType(@Nonnull final FrlIrParser.BooleanTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitIntType(@Nonnull final FrlIrParser.IntTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitBigintType(@Nonnull final FrlIrParser.BigintTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitHalfType(@Nonnull final FrlIrParser.HalfTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.HALF, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitFloatType(@Nonnull final FrlIrParser.FloatTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.FLOAT, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitDoubleType(@Nonnull final FrlIrParser.DoubleTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.DOUBLE, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitStringType(@Nonnull final FrlIrParser.StringTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitBytesType(@Nonnull final FrlIrParser.BytesTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.BYTES, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitUuidType(@Nonnull final FrlIrParser.UuidTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.UUID, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitVersionType(@Nonnull final FrlIrParser.VersionTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.VERSION, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitVoidType(@Nonnull final FrlIrParser.VoidTypeContext ctx) {
        return voidType();
    }

    @Nonnull
    @Override
    public IRecordType visitRecordType(@Nonnull final FrlIrParser.RecordTypeContext ctx) {
        return new IRecordType(false, ctx.recordField()
                .stream()
                .map(this::visitRecordField)
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public IRecordType.Field visitRecordField(@Nonnull final FrlIrParser.RecordFieldContext ctx) {
        final Optional<String> fieldName = Optional.ofNullable(ctx.identifier()).map(FrlIrFragmentVisitor::name);
        final Optional<Integer> fieldNumber = Optional.ofNullable(ctx.INTEGER_LITERAL())
                .map(number -> Integer.parseInt(number.getText()));
        return new IRecordType.Field(type(ctx.type()), fieldName, fieldNumber);
    }

    @Nonnull
    @Override
    public IArrayType visitGenericArrayType(@Nonnull final FrlIrParser.GenericArrayTypeContext ctx) {
        return new IArrayType(false, type(ctx.type()));
    }

    @Nonnull
    @Override
    public IArrayType visitShorthandArrayType(@Nonnull final FrlIrParser.ShorthandArrayTypeContext ctx) {
        return new IArrayType(false, type(ctx.type()));
    }

    /**
     * Creates a vector type. The cast cannot fail: every alternative of the rule for the element type builds a
     * primitive type.
     */
    @Nonnull
    @Override
    public IVectorType visitVectorType(@Nonnull final FrlIrParser.VectorTypeContext ctx) {
        return new IVectorType(false, ((IPrimitiveType)visit(ctx.vectorElementType())).kind(),
                Integer.parseInt(ctx.INTEGER_LITERAL().getText()));
    }

    @Nonnull
    @Override
    public IPrimitiveType visitHalfVectorElementType(@Nonnull final FrlIrParser.HalfVectorElementTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.HALF, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitFloatVectorElementType(@Nonnull final FrlIrParser.FloatVectorElementTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.FLOAT, false);
    }

    @Nonnull
    @Override
    public IPrimitiveType visitDoubleVectorElementType(@Nonnull final FrlIrParser.DoubleVectorElementTypeContext ctx) {
        return new IPrimitiveType(IPrimitiveType.Kind.DOUBLE, false);
    }

    @Nonnull
    @Override
    public IEnumType visitEnumType(@Nonnull final FrlIrParser.EnumTypeContext ctx) {
        return new IEnumType(false, ctx.enumValue()
                .stream()
                .map(this::visitEnumValue)
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public IEnumType.Value visitEnumValue(@Nonnull final FrlIrParser.EnumValueContext ctx) {
        return new IEnumType.Value(name(ctx.identifier()), Integer.parseInt(ctx.integer().getText()));
    }

    /**
     * Creates a function type from {@code function<...>}, which lists the parameter types followed by the result type.
     */
    @Nonnull
    @Override
    public IFunctionType visitGenericFunctionType(@Nonnull final FrlIrParser.GenericFunctionTypeContext ctx) {
        final ImmutableList<IType> types = types(ctx.typeList());
        return new IFunctionType(false, types.subList(0, types.size() - 1), types.get(types.size() - 1));
    }

    @Nonnull
    @Override
    public IFunctionType visitArrowFunctionType(@Nonnull final FrlIrParser.ArrowFunctionTypeContext ctx) {
        final ImmutableList<IType> parameterTypes = ctx.typeList() == null ? ImmutableList.of() : types(ctx.typeList());
        return new IFunctionType(false, parameterTypes, type(ctx.type()));
    }

    @Nonnull
    @Override
    public IStreamType visitStreamType(@Nonnull final FrlIrParser.StreamTypeContext ctx) {
        return new IStreamType(type(ctx.type()));
    }

    //
    // Helpers
    //

    /**
     * Builds the type that a {@code type} rule denotes. The cast cannot fail: every alternative of the rule builds a
     * type.
     */
    @Nonnull
    private IType type(@Nonnull final FrlIrParser.TypeContext ctx) {
        return (IType)visit(ctx);
    }

    @Nonnull
    private ImmutableList<IType> types(@Nonnull final FrlIrParser.TypeListContext ctx) {
        return ctx.type().stream().map(this::type).collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the result type of a function definition or a closure that produces no value.
     */
    @Nonnull
    private static IPrimitiveType voidType() {
        return new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
    }

    /**
     * Returns the inference that types the expressions of the current scope. Expressions occur only in statements, and
     * the scope of every statement has an inference.
     */
    @Nonnull
    private TypeInference inference() {
        final TypeInference inference = scope.inference();
        if (inference == null) {
            throw new IllegalStateException("an expression is inferred only as part of a statement");
        }
        return inference;
    }

    /**
     * Runs a step in a scope nested in the current one, and returns to the current scope afterwards, also if the step
     * fails.
     */
    @Nonnull
    private <R> R inScope(@Nonnull final Scope nestedScope, @Nonnull final Supplier<R> step) {
        scope = nestedScope;
        try {
            return step.get();
        } finally {
            scope = scope.exit();
        }
    }

    /**
     * Returns the name that an identifier denotes, see {@link ParserHelpers#readName}.
     */
    @Nonnull
    private static String name(@Nonnull final FrlIrParser.IdentifierContext ctx) {
        return ParserHelpers.readName(ctx.getText());
    }

    //
    // The visitors of statements and expressions, whose fragments are built once their inference is solved
    //

    /**
     * Infers a statement, one method for each alternative of the {@code statement} rule.
     */
    private final class StatementInferenceVisitor extends FrlIrParserBaseVisitor<PendingStatement> {
        /**
         * The expected type of the value of the statement, if the statement is the last one of a body whose value has
         * an expected type.
         */
        @Nullable
        private final TypeInference.Term expectedValue;

        private StatementInferenceVisitor(@Nullable final TypeInference.Term expectedValue) {
            this.expectedValue = expectedValue;
        }

        @Nonnull
        @Override
        public PendingStatement visitDefinitionStatement(@Nonnull final FrlIrParser.DefinitionStatementContext ctx) {
            final IFunctionDefinition definition = functionDefinition(ctx.functionDefinition());
            return new PendingStatement(() -> definition, null);
        }

        @Nonnull
        @Override
        public PendingStatement visitDeclarationStatement(@Nonnull final FrlIrParser.DeclarationStatementContext ctx) {
            final IFunctionDeclaration declaration = functionDeclaration(ctx.functionDeclaration());
            return new PendingStatement(() -> declaration, null);
        }

        /**
         * Infers an assignment. The variable is declared after its expression is inferred, so the expression cannot
         * refer to it, and the type of the variable is the type of the expression.
         */
        @Nonnull
        @Override
        public PendingStatement visitAssignmentStatement(@Nonnull final FrlIrParser.AssignmentStatementContext ctx) {
            final FrlIrParser.AssignmentContext assignment = ctx.assignment();
            final Inferred value = infer(assignment.expression(),
                    assignment.type() == null ? null : TypeInference.known(type(assignment.type())),
                    "the expression of an assignment must be of the type stated for the variable");
            final String name = name(assignment.identifier());
            scope.declareVariable(name, value.type());
            return new PendingStatement(() -> new IAssignment(name, value.builder().get()), null);
        }

        @Nonnull
        @Override
        public PendingStatement visitExpressionStatement(@Nonnull final FrlIrParser.ExpressionStatementContext ctx) {
            final Inferred value = infer(ctx.expression(), expectedValue,
                    "the value of a body must be of the result type of its function or closure");
            return new PendingStatement(() -> value.builder().get(), value.type());
        }
    }

    /**
     * Infers a primary expression, one method for each alternative of the {@code primaryExpression} and the
     * {@code literal} rules.
     */
    private final class ExpressionInferenceVisitor extends FrlIrParserBaseVisitor<Inferred> {
        /**
         * The type that the context of the expression requires, if any.
         */
        @Nullable
        private final TypeInference.Term expected;
        /**
         * The message of the exception if the expression cannot have the expected type.
         */
        @Nonnull
        private final String mismatch;

        private ExpressionInferenceVisitor(@Nullable final TypeInference.Term expected,
                                           @Nonnull final String mismatch) {
            this.expected = expected;
            this.mismatch = mismatch;
        }

        @Nonnull
        @Override
        public Inferred visitLiteralExpression(@Nonnull final FrlIrParser.LiteralExpressionContext ctx) {
            return fit(visit(ctx.literal()));
        }

        @Nonnull
        @Override
        public Inferred visitIntegerLiteral(@Nonnull final FrlIrParser.IntegerLiteralContext ctx) {
            final String text = (ctx.MINUS() == null ? "" : "-") + ctx.INTEGER_LITERAL().getText();
            return literal(TypeInference.LiteralKind.INTEGER, type -> numericValue(text, type));
        }

        @Nonnull
        @Override
        public Inferred visitFloatingLiteral(@Nonnull final FrlIrParser.FloatingLiteralContext ctx) {
            final String text = (ctx.MINUS() == null ? "" : "-") + ctx.FLOATING_LITERAL().getText();
            return literal(TypeInference.LiteralKind.FLOATING, type -> numericValue(text, type));
        }

        @Nonnull
        @Override
        public Inferred visitStringLiteral(@Nonnull final FrlIrParser.StringLiteralContext ctx) {
            final String value = stringValue(ctx.STRING_LITERAL().getText());
            return literal(TypeInference.LiteralKind.STRING, type -> value);
        }

        @Nonnull
        @Override
        public Inferred visitTrueLiteral(@Nonnull final FrlIrParser.TrueLiteralContext ctx) {
            return literal(TypeInference.LiteralKind.BOOLEAN, type -> Boolean.TRUE);
        }

        @Nonnull
        @Override
        public Inferred visitFalseLiteral(@Nonnull final FrlIrParser.FalseLiteralContext ctx) {
            return literal(TypeInference.LiteralKind.BOOLEAN, type -> Boolean.FALSE);
        }

        @Nonnull
        @Override
        public Inferred visitNilLiteral(@Nonnull final FrlIrParser.NilLiteralContext ctx) {
            return literal(TypeInference.LiteralKind.NIL, type -> null);
        }

        /**
         * Infers the type of a name. A variable has the type it was declared with. A reference to a function has the
         * type of the function, in which new inference variables stand for the type parameters of the function; what
         * they are solved as becomes the type arguments of the reference.
         *
         * <p>A variable that a closure declares can have a type that the inference of the closure's statement has not
         * determined yet. A function nested in the closure is typed by inferences of its own, so it can use such a
         * variable only once the type is known.</p>
         */
        @Nonnull
        @Override
        public Inferred visitNameExpression(@Nonnull final FrlIrParser.NameExpressionContext ctx) {
            final TypeInference inference = inference();
            final String name = name(ctx.identifier());
            final Scope.Binding binding = scope.lookupValue(name)
                    .orElseThrow(() -> new IllegalArgumentException("unknown name"));
            if (binding instanceof Scope.VariableBinding variable) {
                final TypeInference.Term type;
                if (variable.owner() == null || variable.owner() == inference) {
                    type = variable.type();
                } else {
                    Preconditions.checkArgument(TypeInference.isResolved(variable.type()),
                            "a function nested in a closure can use a variable of the closure only once its type is known");
                    type = TypeInference.known(TypeInference.resolve(variable.type()));
                }
                return fit(new Inferred(type, () -> new IVariableReference(name, TypeInference.resolve(type))));
            }
            final Scope.FunctionBinding function = (Scope.FunctionBinding)binding;
            final TypeInference.Instantiation instantiation =
                    inference.instantiate(function.typeParameters(), function.type());
            // an instantiated function type resolves to a function type
            return fit(new Inferred(instantiation.type(), () -> new IFunctionReference(name,
                    instantiation.typeArguments()
                            .stream()
                            .map(TypeInference::resolve)
                            .collect(ImmutableList.toImmutableList()), (IFunctionType)TypeInference.resolve(instantiation.type()))));
        }

        /**
         * Infers the type of a closure. A parameter type or a result type that the closure does not state is a new
         * inference variable. The closure's function type is unified with the expected type before the body is
         * inferred, so that the context determines the types that the closure leaves out and the body sees them; the
         * closure does not need {@link #fit} afterwards. The scope of the closure inherits the inference of the
         * statement that the closure is in, so the body joins it. A result type that neither the closure nor the
         * context states becomes the type of the value of the body, or {@code void} if the body has no value.
         */
        @Nonnull
        @Override
        public Inferred visitClosureExpression(@Nonnull final FrlIrParser.ClosureExpressionContext ctx) {
            final TypeInference inference = inference();
            final FrlIrParser.ClosureContext closure = ctx.closure();
            return inScope(scope.enterBlock(), () -> {
                final FrlIrParser.ClosureSignatureContext signature = closure.closureSignature();
                final ImmutableList<ClosureParameter> parameters = signature == null
                        ? ImmutableList.of()
                        : new ClosureParameterInferenceVisitor().visit(signature.closureParameterClause());
                final IType statedResultType = signature == null || signature.type() == null
                        ? null
                        : type(signature.type());
                final TypeInference.Term resultType = statedResultType == null
                        ? inference.unknown()
                        : TypeInference.known(statedResultType);
                final TypeInference.Term type = inference.function(parameters.stream()
                        .map(ClosureParameter::type)
                        .collect(ImmutableList.toImmutableList()), resultType);
                if (expected != null) {
                    inference.unify(type, expected, mismatch);
                }
                parameters.forEach(parameter -> scope.declareVariable(parameter.name(), parameter.type()));
                final boolean discardsValue = statedResultType != null && statedResultType.isVoid();
                final PendingStatements body = inScope(scope.enterBlock(),
                        () -> statements(closure.statements(), discardsValue ? null : resultType));
                if (statedResultType == null && body.valueType() == null) {
                    inference.unify(resultType, TypeInference.known(voidType()),
                            "the body of a closure must end with an expression of its result type unless that is void");
                }
                return new Inferred(type, () -> new IClosure(parameters.stream()
                        .map(parameter -> new IClosure.Parameter(parameter.name(),
                                TypeInference.resolve(parameter.type())))
                        .collect(ImmutableList.toImmutableList()), TypeInference.resolve(resultType),
                        new IBlock(body.builder().get())));
            });
        }

        /**
         * Requires the type of an expression to be the expected type, if there is one.
         */
        @Nonnull
        private Inferred fit(@Nonnull final Inferred inferred) {
            if (expected != null) {
                inference().unify(inferred.type(), expected, mismatch);
            }
            return inferred;
        }
    }

    /**
     * Returns the parameters of a closure, one method for each alternative of the {@code closureParameterClause} rule.
     * A parameter that does not state its type has a new inference variable as its type.
     */
    private final class ClosureParameterInferenceVisitor
            extends FrlIrParserBaseVisitor<ImmutableList<ClosureParameter>> {
        @Nonnull
        @Override
        public ImmutableList<ClosureParameter> visitParenthesizedClosureParameters(
                @Nonnull final FrlIrParser.ParenthesizedClosureParametersContext ctx) {
            final TypeInference inference = inference();
            return ctx.closureParameter()
                    .stream()
                    .map(parameter -> new ClosureParameter(name(parameter.identifier()), parameter.type() == null
                            ? inference.unknown()
                            : TypeInference.known(type(parameter.type()))))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        @Override
        public ImmutableList<ClosureParameter> visitBareClosureParameters(
                @Nonnull final FrlIrParser.BareClosureParametersContext ctx) {
            final TypeInference inference = inference();
            return ctx.identifier()
                    .stream()
                    .map(identifier -> new ClosureParameter(name(identifier), inference.unknown()))
                    .collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * An expression whose type is being inferred: its type as a term of the inference of its statement, and how to
     * build the expression once the statement is solved and its types are known.
     *
     * @param type the type of the expression
     * @param builder builds the expression; it must be called only after the inference is solved
     */
    private record Inferred(@Nonnull TypeInference.Term type, @Nonnull Supplier<IExpression> builder) {
    }

    /**
     * A statement whose types are being inferred, and how to build it once they are known.
     *
     * @param builder builds the statement; it must be called only after the inference is solved
     * @param valueType the type of the statement's value if the statement is an expression, and {@code null} otherwise
     */
    private record PendingStatement(@Nonnull Supplier<IStatement> builder, @Nullable TypeInference.Term valueType) {
    }

    /**
     * Statements whose types are being inferred, and how to build them once they are known.
     *
     * @param builder builds the statements; it must be called only after the inference is solved
     * @param valueType the type of the value of the statements, the value of the last one if it is an expression, and
     *        {@code null} if they have no value
     */
    private record PendingStatements(@Nonnull Supplier<ImmutableList<IStatement>> builder,
                                     @Nullable TypeInference.Term valueType) {
    }

    /**
     * A parameter of a closure whose type is being inferred.
     *
     * @param name the name of the parameter
     * @param type the type of the parameter, an inference variable if the closure does not state it
     */
    private record ClosureParameter(@Nonnull String name, @Nonnull TypeInference.Term type) {
    }
}
