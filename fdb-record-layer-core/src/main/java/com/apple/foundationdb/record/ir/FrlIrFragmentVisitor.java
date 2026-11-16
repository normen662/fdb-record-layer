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
import com.apple.foundationdb.record.ir.fragments.IAnnotationFragment;
import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IAssignmentFragment;
import com.apple.foundationdb.record.ir.fragments.IBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IClosureFragment;
import com.apple.foundationdb.record.ir.fragments.IConsumeFragment;
import com.apple.foundationdb.record.ir.fragments.IEnumType;
import com.apple.foundationdb.record.ir.fragments.IExpressionFragment;
import com.apple.foundationdb.record.ir.fragments.IForFragment;
import com.apple.foundationdb.record.ir.fragments.IFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionCallFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinitionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IIfFragment;
import com.apple.foundationdb.record.ir.fragments.ILiteralFragment;
import com.apple.foundationdb.record.ir.fragments.IPackageStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IProgramFragment;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeAliasFragment;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IVariableReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IVectorType;
import com.apple.foundationdb.record.ir.fragments.IYieldBreakFragment;
import com.apple.foundationdb.record.ir.fragments.IYieldFragment;
import com.apple.foundationdb.record.ir.fragments.ParserHelpers;
import com.apple.foundationdb.record.ir.generated.FrlIrParser;
import com.apple.foundationdb.record.ir.generated.FrlIrParserBaseVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builds the fragments of an FRL-IR program from its parse tree: {@link #visitProgram} builds the
 * {@link IProgramFragment} of a program, and the visit methods for types build the {@link IType} that a type denotes.
 *
 * <p>The visitor keeps the {@link Scope} of the construct it is in. A function definition declares its type parameters
 * and its parameters in a scope of their own, a block opens a scope for what its statements declare, and leaving a
 * construct returns to the enclosing scope. A name in a type is a type only if it is a type parameter, an extern type,
 * or a type alias in scope, so a misspelled type is an error. Names are resolved once, here: a reference to a function
 * refers to the definition or declaration that its name stands for where the reference is, an extern type to its
 * declaration, and a type alias is replaced by its type. A visitor holds the scope of one traversal, so each parse uses
 * a new visitor.</p>
 *
 * <p>Statements are typed as they are built. The visitor first infers a statement: it declares what the statement
 * declares, and it adds the statement's equations to the {@code TypeInference} of the statement's scope. It then
 * solves the inference and builds the fragments from the solved types. The statements of a closure or a stream block
 * join the inference of the statement that the closure or the stream block is in, so they are built only once that
 * statement is solved; every other statement, including every statement of a function body, is solved and built on
 * its own. The statements of the blocks of an if and a for are typed as the statements around the if or the for: they
 * join the inference of the if or the for where that is the inference of a closure or a stream block, and are solved
 * and built one by one otherwise. A statement and an expression thus produce something to build later rather than a
 * fragment, so they are visited by visitors of their own, one method for each alternative of the grammar.</p>
 */
@API(API.Status.EXPERIMENTAL)
public class FrlIrFragmentVisitor extends FrlIrParserBaseVisitor<IFragment> {

    /**
     * The message of the exception if a callee is not a function of the types of the arguments of a call.
     */
    @Nonnull
    private static final String ARGUMENT_MISMATCH =
            "a call must call a function that is not nullable with arguments of the types of its parameters";

    /**
     * The message of the exception if a yield is not a statement of a stream block.
     */
    @Nonnull
    private static final String YIELD_OUTSIDE_STREAM =
            "a yield must be in a stream block, and not in a function or a closure within one";

    /**
     * The message of the exception if a consume is not a statement of a stream block.
     */
    @Nonnull
    private static final String CONSUME_OUTSIDE_STREAM =
            "a consume must be in a stream block, and not in a function or a closure within one";

    /**
     * The message of the exception if the cases of a consume are not those that a consume can have.
     */
    @Nonnull
    private static final String CONSUME_CASES =
            "a consume has one .value(x) case and one .stopped(reason) case, and at most one .end case besides";

    @Nonnull
    private Scope scope = prelude();

    //
    // Programs and statements
    //

    @Nonnull
    @Override
    public IProgramFragment visitProgram(@Nonnull final FrlIrParser.ProgramContext ctx) {
        return new IProgramFragment(statements(ctx.statements(), null).builder().get());
    }

    /**
     * Returns the outermost scope, in which FRL-IR declares the types that it provides itself, rather than a program:
     * the alias {@code stopReason} of the enum of the reasons why a stream stops, see
     * {@link IConsumeFragment#STOP_REASON_TYPE}.
     */
    @Nonnull
    private static Scope prelude() {
        final Scope root = Scope.root();
        root.declareTypeAlias(IConsumeFragment.STOP_REASON_ALIAS);
        return root;
    }

    /**
     * Infers the statements of a program, a block, a closure, or a stream block. If the last statement is an
     * expression, the expected type of the value of the statements is its context, and its type is the type of the
     * value.
     */
    @Nonnull
    private PendingStatements statements(@Nonnull final FrlIrParser.StatementsContext ctx,
                                         @Nullable final TypeInference.Term expectedValue) {
        final List<FrlIrParser.StatementContext> statementContexts = ctx.statement();
        final ImmutableList.Builder<Supplier<IStatementFragment>> builders = ImmutableList.builder();
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
        final ImmutableList<Supplier<IStatementFragment>> pending = builders.build();
        return new PendingStatements(() -> pending.stream()
                .map(Supplier::get)
                .collect(ImmutableList.toImmutableList()), valueType);
    }

    /**
     * Infers a statement. A statement of a closure or a stream block joins the inference of the statement that the
     * closure or the stream block is in, which its scope carries. Any other statement gets a scope with an inference of
     * its own, and it is inferred, solved, and built right away.
     */
    @Nonnull
    private PendingStatement statement(@Nonnull final FrlIrParser.StatementContext ctx,
                                       @Nullable final TypeInference.Term expectedValue) {
        if (scope.inference() != null) {
            return new StatementInferenceVisitor(expectedValue, false).visit(ctx);
        }
        final TypeInference statementInference = new TypeInference();
        return inScope(scope.enterStatement(statementInference), () -> {
            final PendingStatement pending = new StatementInferenceVisitor(expectedValue, true).visit(ctx);
            statementInference.solve();
            final IStatementFragment statement = pending.builder().get();
            return new PendingStatement(() -> statement, pending.valueType());
        });
    }

    /**
     * Builds a function definition. Its parameters are declared in the scope of the function, which its body is nested
     * in. The scope of the function has no inference, so the statements of the body are typed one by one, also inside a
     * closure, and the type parameters of the function stay inside it.
     */
    @Nonnull
    private IFunctionDefinitionFragment functionDefinition(@Nonnull final FrlIrParser.FunctionDefinitionContext ctx) {
        final ImmutableList<IAnnotationFragment> annotations = annotations(ctx.annotation());
        return function(ctx.functionSignature(), ctx.type(), signature -> {
            signature.parameters()
                    .forEach(parameter -> scope.declareVariable(parameter.name(), TypeInference.known(parameter.type())));
            final IType resultType = signature.resultType();
            final PendingStatements body = inScope(scope.enterBlock(), () -> statements(ctx.block().statements(),
                    resultType.isVoid() ? null : TypeInference.known(resultType)));
            return new IFunctionDefinitionFragment(signature.name(), signature.typeParameters(), signature.parameters(),
                    resultType, new IBlockFragment(body.builder().get()), annotations);
        });
    }

    /**
     * Builds a function declaration. The declaration records the package of the program, which, together with its
     * annotations, says what the function is outside FRL-IR.
     */
    @Nonnull
    private IFunctionDeclarationFragment functionDeclaration(
            @Nonnull final FrlIrParser.FunctionDeclarationContext ctx) {
        final ImmutableList<IAnnotationFragment> annotations = annotations(ctx.annotation());
        return function(ctx.functionSignature(), ctx.type(), signature ->
                new IFunctionDeclarationFragment(signature.name(), signature.typeParameters(), signature.parameters(),
                        signature.resultType(), annotations, scope.packageStatement()));
    }

    /**
     * Builds a function from its signature, what a definition and a declaration have in common. The type parameters
     * are declared in a scope of the function, in which the signature is read and the function is built. The function
     * is declared in the enclosing scope once it is built, so that a reference to it refers to the definition; a body
     * therefore cannot call its own function, and FRL-IR has no recursion.
     *
     * @param resultType the result type of the function, or {@code null} if it returns {@code void}
     * @param builder builds the function, in the scope of the function, from the signature
     */
    @Nonnull
    private <F extends IFunctionFragment> F function(@Nonnull final FrlIrParser.FunctionSignatureContext ctx,
                                                     @Nullable final FrlIrParser.TypeContext resultType,
                                                     @Nonnull final Function<Signature, F> builder) {
        final ImmutableList<IGenericType> typeParameters = typeParameters(ctx.typeParameters());
        final Scope enclosingScope = scope;
        return inScope(scope.enterFunction(typeParameters), () -> {
            final F function = builder.apply(new Signature(name(ctx.identifier()), typeParameters,
                    parameters(ctx.parameter()), resultType == null ? voidType() : type(resultType)));
            enclosingScope.declareFunction(function);
            return function;
        });
    }

    /**
     * Builds a type declaration and declares the extern type in the current scope. Like a function declaration, it
     * records the package of the program.
     */
    @Nonnull
    private ITypeDeclarationFragment typeDeclaration(@Nonnull final FrlIrParser.TypeDeclarationContext ctx) {
        final ITypeDeclarationFragment declaration = new ITypeDeclarationFragment(name(ctx.identifier()),
                annotations(ctx.annotation()), scope.packageStatement());
        scope.declareExternType(declaration);
        return declaration;
    }

    /**
     * Builds a type alias and declares it in the current scope. The type is read before the alias is declared, so it
     * cannot refer to the alias.
     */
    @Nonnull
    private ITypeAliasFragment typeAlias(@Nonnull final FrlIrParser.TypeAliasContext ctx) {
        final ITypeAliasFragment alias = new ITypeAliasFragment(name(ctx.identifier()), type(ctx.type()));
        scope.declareTypeAlias(alias);
        return alias;
    }

    /**
     * Builds the package statement of a program and declares the package, which the outermost scope then holds. Only
     * the first statement of a program can name a package, so the package is in effect for everything the program
     * declares.
     */
    @Nonnull
    private IPackageStatementFragment packageStatement(@Nonnull final FrlIrParser.PackageStatementContext ctx) {
        final FrlIrParser.StatementsContext statements = (FrlIrParser.StatementsContext)ctx.getParent();
        Preconditions.checkArgument(statements.getParent() instanceof FrlIrParser.ProgramContext
                        && statements.statement(0).equals(ctx),
                "a package statement can only be the first statement of a program");
        final FrlIrParser.PackageDeclarationContext declaration = ctx.packageDeclaration();
        final IPackageStatementFragment packageStatement =
                new IPackageStatementFragment(name(declaration.identifier()), annotations(declaration.annotation()));
        scope.declarePackage(packageStatement);
        return packageStatement;
    }

    @Nonnull
    @Override
    public IAnnotationFragment visitAnnotation(@Nonnull final FrlIrParser.AnnotationContext ctx) {
        return new IAnnotationFragment(name(ctx.identifier()), ctx.annotationArgument()
                .stream()
                .map(this::visitAnnotationArgument)
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public IAnnotationFragment.Argument visitAnnotationArgument(
            @Nonnull final FrlIrParser.AnnotationArgumentContext ctx) {
        return new IAnnotationFragment.Argument(ctx.identifier() == null ? null : name(ctx.identifier()),
                stringValue(ctx.STRING_LITERAL().getText()));
    }

    @Nonnull
    private ImmutableList<IAnnotationFragment> annotations(@Nonnull final List<FrlIrParser.AnnotationContext> ctx) {
        return ctx.stream().map(this::visitAnnotation).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public IFunctionDefinitionFragment.Parameter visitParameter(@Nonnull final FrlIrParser.ParameterContext ctx) {
        return new IFunctionDefinitionFragment.Parameter(name(ctx.identifier()), type(ctx.type()));
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
    private ImmutableList<IFunctionDefinitionFragment.Parameter> parameters(
            @Nonnull final List<FrlIrParser.ParameterContext> ctx) {
        return ctx.stream().map(this::visitParameter).collect(ImmutableList.toImmutableList());
    }

    //
    // Expressions
    //

    /**
     * Infers the type of an expression, as the overload for a primary expression and its postfixes does.
     */
    @Nonnull
    private Inferred infer(@Nonnull final FrlIrParser.ExpressionContext ctx,
                           @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        return infer(ctx.primaryExpression(), ctx.postfix(), expected, mismatch);
    }

    /**
     * Infers the type of the expression in front of the block of an if, a for, or a consume, whose calls take no
     * trailing closure, as the overload for a primary expression and its postfixes does.
     */
    @Nonnull
    private Inferred infer(@Nonnull final FrlIrParser.ConditionExpressionContext ctx,
                           @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        return infer(ctx.primaryExpression(), ctx.conditionPostfix(), expected, mismatch);
    }

    /**
     * Infers the type of an expression, a primary expression followed by postfixes, each of which applies to the
     * expression before it: a call, a trailing closure, or a dot call. The expected type, if there is one, is the
     * context of the expression: the type stated for a variable, the type of a parameter, or the result type of a body.
     * It is the context of the last postfix, the one whose result the expression is.
     *
     * @param postfixes the postfixes, of the {@code postfix} or the {@code conditionPostfix} rule
     * @param mismatch the message of the exception if the expression cannot have the expected type
     */
    @Nonnull
    private Inferred infer(@Nonnull final FrlIrParser.PrimaryExpressionContext primary,
                           @Nonnull final List<? extends ParseTree> postfixes,
                           @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        if (postfixes.isEmpty()) {
            return new ExpressionInferenceVisitor(expected, mismatch).visit(primary);
        }
        // what a postfix applies to has no context of its own: its type follows from the call
        Inferred expression = new ExpressionInferenceVisitor(null, mismatch).visit(primary);
        final int last = postfixes.size() - 1;
        for (int i = 0; i <= last; i++) {
            expression = new PostfixInferenceVisitor(expression, i == last ? expected : null, mismatch)
                    .visit(postfixes.get(i));
        }
        return expression;
    }

    /**
     * Infers the type of a call: the callee must be a function of the types of the arguments, and the result type of
     * that function is the type of the call. The callee, and the expected type of the call, are unified with the
     * function type before the arguments are inferred, so that a closure among the arguments, a trailing closure
     * included, has the type of its parameter as its context.
     *
     * @param arguments the arguments in order, each of which is inferred given the type of its parameter
     */
    @Nonnull
    private Inferred inferCall(@Nonnull final Inferred callee,
                               @Nonnull final List<Function<TypeInference.Term, Inferred>> arguments,
                               @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final TypeInference inference = inference();
        final ImmutableList<TypeInference.Term> parameterTypes = arguments.stream()
                .map(argument -> inference.unknown())
                .collect(ImmutableList.toImmutableList());
        final TypeInference.Term resultType = inference.unknown();
        inference.unify(callee.type(), inference.function(parameterTypes, resultType), ARGUMENT_MISMATCH);
        if (expected != null) {
            inference.unify(resultType, expected, mismatch);
        }
        final ImmutableList.Builder<Inferred> inferredArguments = ImmutableList.builder();
        for (int i = 0; i < parameterTypes.size(); i++) {
            inferredArguments.add(arguments.get(i).apply(parameterTypes.get(i)));
        }
        final ImmutableList<Inferred> builtArguments = inferredArguments.build();
        return new Inferred(resultType, () -> new IFunctionCallFragment(callee.builder().get(), builtArguments.stream()
                .map(argument -> argument.builder().get())
                .collect(ImmutableList.toImmutableList())));
    }

    /**
     * Returns the arguments of a call: those between the parentheses, if there are parentheses, followed by the
     * trailing closure, if there is one. Each argument is inferred with the type of its parameter as its context, so a
     * trailing closure, like every closure, has that type before its body is inferred.
     */
    @Nonnull
    private ImmutableList<Function<TypeInference.Term, Inferred>> callArguments(
            @Nonnull final FrlIrParser.CallArgumentsContext ctx) {
        final ImmutableList.Builder<Function<TypeInference.Term, Inferred>> arguments = ImmutableList.builder();
        if (ctx.arguments() != null) {
            arguments.addAll(callArguments(ctx.arguments()));
        }
        final FrlIrParser.ClosureContext trailingClosure = ctx.closure();
        if (trailingClosure != null) {
            arguments.add(parameterType ->
                    new ExpressionInferenceVisitor(parameterType, ARGUMENT_MISMATCH).inferClosure(trailingClosure));
        }
        return arguments.build();
    }

    /**
     * Returns the arguments between the parentheses of a call, each of which is inferred with the type of its parameter
     * as its context.
     */
    @Nonnull
    private ImmutableList<Function<TypeInference.Term, Inferred>> callArguments(
            @Nonnull final FrlIrParser.ArgumentsContext ctx) {
        return ctx.expression()
                .stream()
                .<Function<TypeInference.Term, Inferred>>map(argument ->
                        parameterType -> infer(argument, parameterType, ARGUMENT_MISMATCH))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the type of a reference to a function, in which new inference variables stand for the type parameters of
     * the function; what they are solved as becomes the type arguments of the reference. The reference refers to the
     * function that the name stands for where the reference is.
     */
    @Nonnull
    private Inferred functionReference(@Nonnull final Scope.FunctionBinding function) {
        final TypeInference.Instantiation instantiation =
                inference().instantiate(function.typeParameters(), function.type());
        // an instantiated function type resolves to a function type
        return new Inferred(instantiation.type(), () -> new IFunctionReferenceFragment(function.function(),
                instantiation.typeArguments()
                        .stream()
                        .map(TypeInference::resolve)
                        .collect(ImmutableList.toImmutableList()),
                (IFunctionType)TypeInference.resolve(instantiation.type())));
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
            return new ILiteralFragment(valueOfType.apply(resolvedType), resolvedType);
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
     * Resolves the name of a type: to the type parameter that the nearest enclosing function declares by that name, or
     * to the extern type or the type alias that the nearest enclosing scope declares by it; a type alias resolves to
     * the type that it stands for. Its nullability is applied by the {@code valueType} around it, which rejects the
     * {@code ?} after an alias of a nullable type.
     */
    @Nonnull
    @Override
    public IType visitNamedType(@Nonnull final FrlIrParser.NamedTypeContext ctx) {
        return scope.lookupType(name(ctx.identifier()))
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
        /**
         * Whether the statement has an inference of its own, as a statement of a program or of a function body has,
         * rather than joining the inference of a closure or a stream block. The statements of the blocks of an if or a
         * for are then typed one by one as well, so that a statement in such a block is typed as it would be in place
         * of the if or the for.
         */
        private final boolean hasOwnInference;

        private StatementInferenceVisitor(@Nullable final TypeInference.Term expectedValue,
                                          final boolean hasOwnInference) {
            this.expectedValue = expectedValue;
            this.hasOwnInference = hasOwnInference;
        }

        @Nonnull
        @Override
        public PendingStatement visitFunctionDefinitionStatement(
                @Nonnull final FrlIrParser.FunctionDefinitionStatementContext ctx) {
            return PendingStatement.built(functionDefinition(ctx.functionDefinition()));
        }

        @Nonnull
        @Override
        public PendingStatement visitFunctionDeclarationStatement(
                @Nonnull final FrlIrParser.FunctionDeclarationStatementContext ctx) {
            return PendingStatement.built(functionDeclaration(ctx.functionDeclaration()));
        }

        @Nonnull
        @Override
        public PendingStatement visitTypeDeclarationStatement(
                @Nonnull final FrlIrParser.TypeDeclarationStatementContext ctx) {
            return PendingStatement.built(typeDeclaration(ctx.typeDeclaration()));
        }

        @Nonnull
        @Override
        public PendingStatement visitTypeAliasStatement(@Nonnull final FrlIrParser.TypeAliasStatementContext ctx) {
            return PendingStatement.built(typeAlias(ctx.typeAlias()));
        }

        @Nonnull
        @Override
        public PendingStatement visitPackageStatement(@Nonnull final FrlIrParser.PackageStatementContext ctx) {
            return PendingStatement.built(packageStatement(ctx));
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
            return new PendingStatement(() -> new IAssignmentFragment(name, value.builder().get()), null);
        }

        /**
         * Infers a yield. The element type of the stream of the enclosing stream block is the context of the value, so
         * that, as for an argument, a literal or a closure takes its type from it.
         */
        @Nonnull
        @Override
        public PendingStatement visitYieldStatement(@Nonnull final FrlIrParser.YieldStatementContext ctx) {
            final TypeInference.Term elementType = scope.yieldElementType();
            if (elementType == null) {
                throw new IllegalArgumentException(YIELD_OUTSIDE_STREAM);
            }
            final Inferred value = infer(ctx.yieldValue().expression(), elementType,
                    "a yielded value must be of the element type of its stream");
            return new PendingStatement(() -> new IYieldFragment(value.builder().get()), null);
        }

        /**
         * Infers a yield break, which can be where a yield can be. Its reason, if it has one, is a {@code stopReason},
         * which is the context of the reason.
         */
        @Nonnull
        @Override
        public PendingStatement visitYieldBreakStatement(@Nonnull final FrlIrParser.YieldBreakStatementContext ctx) {
            if (scope.yieldElementType() == null) {
                throw new IllegalArgumentException(YIELD_OUTSIDE_STREAM);
            }
            final FrlIrParser.ExpressionContext reason = ctx.yieldBreak().expression();
            if (reason == null) {
                return PendingStatement.built(new IYieldBreakFragment(null));
            }
            final Inferred inferredReason = infer(reason, TypeInference.known(IConsumeFragment.STOP_REASON_TYPE),
                    "the reason of a yield break must be a stopReason");
            return new PendingStatement(() -> new IYieldBreakFragment(inferredReason.builder().get()), null);
        }

        @Nonnull
        @Override
        public PendingStatement visitIfStatement(@Nonnull final FrlIrParser.IfStatementContext ctx) {
            final Supplier<IIfFragment> conditional = conditional(ctx.conditional());
            return new PendingStatement(conditional::get, null);
        }

        /**
         * Infers an if, also one that follows an {@code else}, which becomes the only statement of the else block. The
         * condition is a boolean, which is its context. The condition of an else if joins the inference of the if, as
         * the condition of the if does; the two conditions share no inference variables, so their types are those that
         * each would have on its own.
         */
        @Nonnull
        private Supplier<IIfFragment> conditional(@Nonnull final FrlIrParser.ConditionalContext ctx) {
            final Inferred condition = infer(ctx.conditionExpression(),
                    TypeInference.known(new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false)),
                    "the condition of an if must be a boolean");
            final Supplier<IBlockFragment> thenBlock = block(ctx.thenBlock);
            final Supplier<IBlockFragment> elseBlock;
            if (ctx.elseIf != null) {
                final Supplier<IIfFragment> elseIf = inScope(scope.enterBlock(), () -> conditional(ctx.elseIf));
                elseBlock = () -> new IBlockFragment(ImmutableList.of(elseIf.get()));
            } else if (ctx.elseBlock != null) {
                elseBlock = block(ctx.elseBlock);
            } else {
                elseBlock = null;
            }
            return () -> new IIfFragment(condition.builder().get(), thenBlock.get(),
                    elseBlock == null ? null : elseBlock.get());
        }

        /**
         * Infers a for loop. The sequence is an array, whose element type is the type of the loop variable. The
         * variable is declared after the sequence is inferred, so the sequence cannot refer to it, and it is declared
         * in a scope of the loop, which the body is nested in, as the parameters of a closure are. If the loop has an
         * inference of its own, the statements of the body are typed one by one, so the inference of the loop is
         * solved before them, and the variable has the type that the sequence determines without the body.
         */
        @Nonnull
        @Override
        public PendingStatement visitForStatement(@Nonnull final FrlIrParser.ForStatementContext ctx) {
            final FrlIrParser.ForLoopContext loop = ctx.forLoop();
            final TypeInference inference = inference();
            final TypeInference.Term elementType = inference.unknown();
            final Inferred sequence = infer(loop.conditionExpression(), inference.array(elementType),
                    "a for loop must iterate over an array that is not nullable");
            final TypeInference.Term variableType;
            if (hasOwnInference) {
                inference.solve();
                variableType = TypeInference.known(TypeInference.resolve(elementType));
            } else {
                variableType = elementType;
            }
            final String variable = name(loop.identifier());
            final Supplier<IBlockFragment> body = inScope(blockScope(), () -> {
                scope.declareVariable(variable, variableType);
                return block(loop.block());
            });
            return new PendingStatement(() -> new IForFragment(variable, sequence.builder().get(), body.get()), null);
        }

        /**
         * Infers a consume, which can be where a yield can be, since its stopped case stops or ends the stream block
         * that it is in. The consumed expression is a stream, whose element type is the type of the binding of the
         * value case; the binding of the stopped case is a {@code stopReason}. The consume itself checks that the
         * stopped case ends with a yield break on every path once it is built.
         */
        @Nonnull
        @Override
        public PendingStatement visitConsumeStatement(@Nonnull final FrlIrParser.ConsumeStatementContext ctx) {
            if (scope.yieldElementType() == null) {
                throw new IllegalArgumentException(CONSUME_OUTSIDE_STREAM);
            }
            final FrlIrParser.ConsumeLoopContext loop = ctx.consumeLoop();
            final TypeInference inference = inference();
            final TypeInference.Term elementType = inference.unknown();
            final Inferred stream = infer(loop.conditionExpression(), inference.stream(elementType),
                    "a consume must consume a stream");
            Supplier<IConsumeFragment.Case> valueCase = null;
            Supplier<IConsumeFragment.Case> endCase = null;
            Supplier<IConsumeFragment.Case> stoppedCase = null;
            for (final FrlIrParser.ConsumeCaseContext consumeCase : loop.consumeCase()) {
                switch (name(consumeCase.kind)) {
                    case "value" -> {
                        Preconditions.checkArgument(valueCase == null && consumeCase.binding != null, CONSUME_CASES);
                        valueCase = consumeCase(consumeCase, elementType);
                    }
                    case "end" -> {
                        Preconditions.checkArgument(endCase == null && consumeCase.binding == null, CONSUME_CASES);
                        endCase = consumeCase(consumeCase, null);
                    }
                    case "stopped" -> {
                        Preconditions.checkArgument(stoppedCase == null && consumeCase.binding != null, CONSUME_CASES);
                        stoppedCase = consumeCase(consumeCase, TypeInference.known(IConsumeFragment.STOP_REASON_TYPE));
                    }
                    default -> throw new IllegalArgumentException(CONSUME_CASES);
                }
            }
            Preconditions.checkArgument(valueCase != null && stoppedCase != null, CONSUME_CASES);
            final Supplier<IConsumeFragment.Case> value = valueCase;
            final Supplier<IConsumeFragment.Case> end = endCase;
            final Supplier<IConsumeFragment.Case> stopped = stoppedCase;
            return new PendingStatement(() -> new IConsumeFragment(stream.builder().get(), value.get(),
                    end == null ? null : end.get(), stopped.get()), null);
        }

        /**
         * Infers a case of a consume. Its binding, if it has one, is declared with the given type in a scope of the
         * case, which the statements of the case are nested in.
         */
        @Nonnull
        private Supplier<IConsumeFragment.Case> consumeCase(@Nonnull final FrlIrParser.ConsumeCaseContext ctx,
                                                            @Nullable final TypeInference.Term bindingType) {
            final String binding = ctx.binding == null ? null : name(ctx.binding);
            final PendingStatements statements = inScope(scope.enterBlock(), () -> {
                if (binding != null) {
                    scope.declareVariable(binding, Objects.requireNonNull(bindingType));
                }
                return inScope(scope.enterBlock(), () -> statements(ctx.statements(), null));
            });
            return () -> new IConsumeFragment.Case(binding, new IBlockFragment(statements.builder().get()));
        }

        /**
         * Infers the statements of a block of an if or a for. The block has a scope of its own, which inherits the
         * element type of yields of the current scope, since the block runs as part of the block that the statement is
         * in. The statements of the block join the inference of the statement, unless the statement has an inference
         * of its own; they are then typed one by one, in a scope without an inference.
         */
        @Nonnull
        private Supplier<IBlockFragment> block(@Nonnull final FrlIrParser.BlockContext ctx) {
            final PendingStatements statements = inScope(blockScope(), () -> statements(ctx.statements(), null));
            return () -> new IBlockFragment(statements.builder().get());
        }

        /**
         * Returns the scope of a block of the statement, or of the scope that declares the variable of a for loop
         * around its body, see {@link #block}.
         */
        @Nonnull
        private Scope blockScope() {
            return hasOwnInference ? scope.enterBlockWithoutInference() : scope.enterBlock();
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
         * <p>A variable that a closure or a stream block declares can have a type that the inference of their statement
         * has not determined yet. A function nested in the closure or the stream block is typed by inferences of its
         * own, so it can use such a variable only once the type is known.</p>
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
                            "a function can use a variable of an enclosing closure or stream block only once its type "
                                    + "is known");
                    type = TypeInference.known(TypeInference.resolve(variable.type()));
                }
                return fit(new Inferred(type, () -> new IVariableReferenceFragment(name, TypeInference.resolve(type))));
            }
            return fit(functionReference((Scope.FunctionBinding)binding));
        }

        @Nonnull
        @Override
        public Inferred visitClosureExpression(@Nonnull final FrlIrParser.ClosureExpressionContext ctx) {
            return inferClosure(ctx.closure());
        }

        /**
         * Infers the type of a stream block, {@code stream { … }}, a stream of a new inference variable. The stream
         * type is unified with the expected type before the block is inferred, so that the context determines the
         * element type and every yield has it as the context of its value; without a context, the yielded values
         * determine it. The scope of the block inherits the inference of the statement that the stream block is in, so
         * the statements of the block join it, as those of a closure do. The value of the block is discarded, so its
         * last expression has no context.
         */
        @Nonnull
        @Override
        public Inferred visitStreamExpression(@Nonnull final FrlIrParser.StreamExpressionContext ctx) {
            final TypeInference inference = inference();
            final TypeInference.Term elementType = inference.unknown();
            final TypeInference.Term type = inference.stream(elementType);
            if (expected != null) {
                inference.unify(type, expected, mismatch);
            }
            final PendingStatements block = inScope(scope.enterStream(elementType),
                    () -> statements(ctx.streamBlock().block().statements(), null));
            return new Inferred(type, () -> new IStreamBlockFragment(TypeInference.resolve(elementType),
                    new IBlockFragment(block.builder().get())));
        }

        /**
         * Infers the type of a closure, also of a trailing closure. A parameter type or a result type that the closure
         * does not state is a new inference variable. The closure's function type is unified with the expected type
         * before the body is inferred, so that the context determines the types that the closure leaves out and the
         * body sees them; the closure does not need {@link #fit} afterwards. The scope of the closure inherits the
         * inference of the statement that the closure is in, so the body joins it, but not the element type of a stream
         * block around the closure: a closure cannot yield. A result type that neither the closure nor the context
         * states becomes the type of the value of the body, or {@code void} if the body has no value.
         */
        @Nonnull
        private Inferred inferClosure(@Nonnull final FrlIrParser.ClosureContext closure) {
            final TypeInference inference = inference();
            return inScope(scope.enterClosure(), () -> {
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
                return new Inferred(type, () -> new IClosureFragment(parameters.stream()
                        .map(parameter -> new IClosureFragment.Parameter(parameter.name(),
                                TypeInference.resolve(parameter.type())))
                        .collect(ImmutableList.toImmutableList()), TypeInference.resolve(resultType),
                        new IBlockFragment(body.builder().get())));
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
     * Infers a postfix of an expression, one method for each alternative of the {@code postfix} and the
     * {@code conditionPostfix} rules. Every postfix is a call: a call of the expression before it, with its arguments
     * and, outside the expression in front of a block, its trailing closure, or a dot call of a function whose first
     * argument is the expression before it.
     */
    private final class PostfixInferenceVisitor extends FrlIrParserBaseVisitor<Inferred> {
        /**
         * The expression that the postfix applies to.
         */
        @Nonnull
        private final Inferred before;
        /**
         * The type that the context of the call requires, if any.
         */
        @Nullable
        private final TypeInference.Term expected;
        /**
         * The message of the exception if the call cannot have the expected type.
         */
        @Nonnull
        private final String mismatch;

        private PostfixInferenceVisitor(@Nonnull final Inferred before, @Nullable final TypeInference.Term expected,
                                        @Nonnull final String mismatch) {
            this.before = before;
            this.expected = expected;
            this.mismatch = mismatch;
        }

        @Nonnull
        @Override
        public Inferred visitCallPostfix(@Nonnull final FrlIrParser.CallPostfixContext ctx) {
            return inferCall(before, callArguments(ctx.callArguments()), expected, mismatch);
        }

        @Nonnull
        @Override
        public Inferred visitDotCallPostfix(@Nonnull final FrlIrParser.DotCallPostfixContext ctx) {
            return dotCall(ctx.identifier(), callArguments(ctx.callArguments()));
        }

        @Nonnull
        @Override
        public Inferred visitConditionCallPostfix(@Nonnull final FrlIrParser.ConditionCallPostfixContext ctx) {
            return inferCall(before, callArguments(ctx.arguments()), expected, mismatch);
        }

        @Nonnull
        @Override
        public Inferred visitConditionDotCallPostfix(@Nonnull final FrlIrParser.ConditionDotCallPostfixContext ctx) {
            return dotCall(ctx.identifier(), callArguments(ctx.arguments()));
        }

        /**
         * Infers a dot call, {@code x.f(1)}, which calls the function {@code f} with {@code x} as its first argument. The
         * name is looked up among the functions only, so that a variable of the name does not hide the function.
         * {@code x} is inferred before the function is known, so its type gets no context from the function's first
         * parameter until it is unified with that parameter.
         *
         * @param arguments the arguments after the first one, in order
         */
        @Nonnull
        private Inferred dotCall(@Nonnull final FrlIrParser.IdentifierContext function,
                                 @Nonnull final List<Function<TypeInference.Term, Inferred>> arguments) {
            final Scope.FunctionBinding binding = scope.lookupFunction(name(function))
                    .orElseThrow(() -> new IllegalArgumentException("unknown function"));
            final Function<TypeInference.Term, Inferred> receiver = parameterType -> {
                inference().unify(before.type(), parameterType, ARGUMENT_MISMATCH);
                return before;
            };
            return inferCall(functionReference(binding), ImmutableList.<Function<TypeInference.Term, Inferred>>builder()
                    .add(receiver)
                    .addAll(arguments)
                    .build(), expected, mismatch);
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
    private record Inferred(@Nonnull TypeInference.Term type, @Nonnull Supplier<IExpressionFragment> builder) {
    }

    /**
     * A statement whose types are being inferred, and how to build it once they are known.
     *
     * @param builder builds the statement; it must be called only after the inference is solved
     * @param valueType the type of the statement's value if the statement is an expression, and {@code null} otherwise
     */
    private record PendingStatement(@Nonnull Supplier<IStatementFragment> builder,
                                    @Nullable TypeInference.Term valueType) {
        /**
         * Returns a statement that is built already, which involves no inference and has no value: a definition, a
         * declaration, a type alias, or a yield break without a reason.
         */
        @Nonnull
        static PendingStatement built(@Nonnull final IStatementFragment statement) {
            return new PendingStatement(() -> statement, null);
        }
    }

    /**
     * Statements whose types are being inferred, and how to build them once they are known.
     *
     * @param builder builds the statements; it must be called only after the inference is solved
     * @param valueType the type of the value of the statements, the value of the last one if it is an expression, and
     *        {@code null} if they have no value
     */
    private record PendingStatements(@Nonnull Supplier<ImmutableList<IStatementFragment>> builder,
                                     @Nullable TypeInference.Term valueType) {
    }

    /**
     * The signature of a function, read in the scope of the function.
     *
     * @param name the name of the function
     * @param typeParameters the type parameters of the function, in order
     * @param parameters the parameters of the function, in order
     * @param resultType the result type of the function, {@code void} if it states none
     */
    private record Signature(@Nonnull String name, @Nonnull ImmutableList<IGenericType> typeParameters,
                             @Nonnull ImmutableList<IFunctionDefinitionFragment.Parameter> parameters,
                             @Nonnull IType resultType) {
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
