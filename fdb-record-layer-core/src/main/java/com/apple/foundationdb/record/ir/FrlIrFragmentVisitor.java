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
import com.apple.foundationdb.record.ir.fragments.IFieldAccessFragment;
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
import com.apple.foundationdb.record.ir.fragments.IOverloadedFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IPackageStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IProgramFragment;
import com.apple.foundationdb.record.ir.fragments.IRecordConstructorFragment;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IReturnFragment;
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
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

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
 * <p>The visitor types each statement in three steps, see {@code TypeInference} for the details and an example:</p>
 * <ol>
 *     <li>Infer: the visitor examines the statement. It declares the names of the statement in the scope, and it
 *     states the equations of the statement in the {@code TypeInference} of the scope. The result is a builder, not a
 *     fragment, because the types are not known yet.</li>
 *     <li>Solve: the visitor calls {@code solve()} on the inference.</li>
 *     <li>Build: the visitor calls the builder. The builder resolves the terms, and it makes the fragments.</li>
 * </ol>
 * A statement of a program or of a function body gets its own inference, and the visitor builds it immediately. A
 * statement of a closure or a stream block uses the inference of the statement that the closure or the stream block is
 * in. The visitor builds it together with that statement. The statements of the blocks of an if or a for are typed as
 * the statements around the if or the for. The inference of a statement or an expression gives a builder, not a
 * fragment. So separate visitors infer them, with one method for each alternative of the grammar:
 * {@code StatementInferenceVisitor}, {@code ExpressionInferenceVisitor}, {@code PostfixInferenceVisitor}, and
 * {@code ClosureParameterInferenceVisitor}.
 *
 * <p>An operator is a call of the function that an {@code @operator} annotation in scope gives it, so the visitor
 * infers it as a call, without labels. A call or a reference of an overloaded function refers to one of its variants,
 * which the inference chooses from the types of the arguments and the context, see {@code TypeInference.choose}.</p>
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
     * The message of the exception if an argument of a call of a declared function does not have the label of its
     * parameter.
     */
    @Nonnull
    private static final String ARGUMENT_LABELS =
            "an argument must have the label of its parameter, and no label if the parameter has none";

    /**
     * The message of the exception if a call of a function value has a label.
     */
    @Nonnull
    private static final String FUNCTION_VALUE_LABELS = "the arguments of a call of a function value have no labels";

    /**
     * The label that means "no label" in a parameter, {@code _} without backticks.
     */
    @Nonnull
    private static final String NO_LABEL = "_";

    /**
     * The form of a floating-point literal that stands for two field accesses by ordinal, as {@code 0.1} in
     * {@code x.0.1}: two whole numbers with a point between them.
     */
    @Nonnull
    private static final Pattern ORDINAL_PAIR = Pattern.compile("[0-9]+\\.[0-9]+");

    /**
     * The message of the exception if a yield is not a statement of a stream block.
     */
    @Nonnull
    private static final String YIELD_OUTSIDE_STREAM =
            "a yield must be in a stream block, and not in a function or a closure within one";

    /**
     * The message of the exception if a return is not in the body of a function or a closure.
     */
    @Nonnull
    private static final String RETURN_OUTSIDE_BODY =
            "a return must be in a function or a closure, and not in a stream block, which a yield break ends";

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

    /**
     * The name of the annotation that makes a function the function of an operator, {@code @operator("+")}.
     */
    @Nonnull
    private static final String OPERATOR_ANNOTATION = "operator";

    /**
     * The message of the exception if an {@code @operator} annotation names no operator of FRL-IR, or one with another
     * number of operands than the function has parameters.
     */
    @Nonnull
    private static final String OPERATOR_ANNOTATION_FORM = "an @operator annotation has one argument without a "
            + "label: an operator whose number of operands is the number of parameters of the function";

    /**
     * The message of the exception if an {@code @operator} annotation is not on a function that is not a variant.
     */
    @Nonnull
    private static final String OPERATOR_ANNOTATION_PLACE =
            "an @operator annotation can only be on a function that is not a variant";

    /**
     * The message of the exception if an operator has no function in scope.
     */
    @Nonnull
    private static final String NO_OPERATOR_FUNCTION =
            "an operator needs a function in scope that an @operator annotation gives to it";

    /**
     * The message of the exception if type arguments are given to something that takes none, or in another number.
     */
    @Nonnull
    private static final String TYPE_ARGUMENTS =
            "only a generic or an overloaded function takes type arguments, one for each of its type parameters";

    /**
     * The name after {@code record.} in the long form of a record constructor, {@code record.of(1, 2)}. It is not a
     * keyword, so it can still name a function or a variable.
     */
    @Nonnull
    private static final String RECORD_CONSTRUCTOR_MEMBER = "of";

    /**
     * The message of the exception if the type arguments of a record constructor do not state one field for each
     * value.
     */
    @Nonnull
    private static final String RECORD_CONSTRUCTOR_VALUES =
            "the type arguments of a record constructor state one field for each of its values";

    /**
     * The message of the exception if a value of a record constructor with type arguments does not have the label
     * that the call rule requires.
     */
    @Nonnull
    private static final String RECORD_CONSTRUCTOR_LABELS = "with type arguments, a value of a record constructor has "
            + "the name of its field as its label, and no label if the field has none";

    /**
     * The message of the exception if a value of a record constructor does not have the type that its type arguments
     * state for its field.
     */
    @Nonnull
    private static final String RECORD_CONSTRUCTOR_VALUE_MISMATCH =
            "a value of a record constructor must be of the type that the type arguments state for its field";

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
     * Infers the statements of a program, a block, a closure, or a stream block. As in Swift, a single expression is
     * the value of its statements: if there is exactly one statement and it is an expression, the expected type of the
     * value of the statements is its context, and its type is the type of the value. Other statements have no value.
     */
    @Nonnull
    private PendingStatements statements(@Nonnull final FrlIrParser.StatementsContext ctx,
                                         @Nullable final TypeInference.Term expectedValue) {
        final List<FrlIrParser.StatementContext> statementContexts = ctx.statement();
        if (statementContexts.size() == 1) {
            final PendingStatement only = statement(statementContexts.get(0), expectedValue);
            return new PendingStatements(() -> ImmutableList.of(only.builder().get()), only.valueType());
        }
        final ImmutableList.Builder<Supplier<IStatementFragment>> builders = ImmutableList.builder();
        for (final FrlIrParser.StatementContext statementContext : statementContexts) {
            builders.add(statement(statementContext, null).builder());
        }
        final ImmutableList<Supplier<IStatementFragment>> pending = builders.build();
        return new PendingStatements(() -> pending.stream()
                .map(Supplier::get)
                .collect(ImmutableList.toImmutableList()), null);
    }

    /**
     * Infers a statement. If the current scope has an inference, the statement uses this inference. This is the case
     * in a closure, in a stream block, and in a block of an if or a for that uses the inference of its statement. The
     * builder of the statement then runs when the visitor builds the statement that owns the inference. If the current
     * scope has no inference, the statement gets a new inference in a new statement scope. The visitor then infers,
     * solves, and builds the statement immediately.
     *
     * <p>Example: in a function body, each statement gets a new inference. In {@code run { let a = 42; h(a) }}, the two
     * statements of the closure use the inference of the statement {@code run { ... }}.</p>
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
        return function(ctx.functionSignature(), ctx.type(), signature ->
                new IFunctionDefinitionFragment(signature.name(), signature.typeParameters(), signature.parameters(),
                        signature.resultType(), functionBody(signature, ctx.block()), annotations));
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
     * therefore cannot call its own function, and FRL-IR has no recursion. An {@code @operator} annotation of the
     * function declares its operator there too.
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
            declareOperators(enclosingScope, function.annotations(), enclosingScope.declareFunction(function),
                    function.parameters().size());
            return function;
        });
    }

    /**
     * Infers the body of a function definition, in the scope of the function: the parameters become variables of the
     * body, and the result type is the context of a single expression of the body and of the values of its returns.
     */
    @Nonnull
    private IBlockFragment functionBody(@Nonnull final Signature signature,
                                        @Nonnull final FrlIrParser.BlockContext block) {
        signature.parameters()
                .forEach(parameter -> scope.declareVariable(parameter.name(), TypeInference.known(parameter.type())));
        final IType resultType = signature.resultType();
        final Scope.ReturnTarget target = new Scope.ReturnTarget(TypeInference.known(resultType));
        final PendingStatements body = inScope(scope.enterBody(target), () -> statements(block.statements(),
                resultType.isVoid() ? null : TypeInference.known(resultType)));
        return new IBlockFragment(body.builder().get());
    }

    /**
     * Builds an overloaded function and declares it in the current scope, which its variants must then be declared in.
     * Like a function, it reads its parameters in a scope of its type parameters. An {@code @operator} annotation of it
     * declares its operator, which then calls one of its variants.
     */
    @Nonnull
    private IOverloadedFunctionFragment overloadedFunction(
            @Nonnull final FrlIrParser.OverloadedFunctionDeclarationContext ctx) {
        final ImmutableList<IAnnotationFragment> annotations = annotations(ctx.annotation());
        final FrlIrParser.FunctionSignatureContext signature = ctx.functionSignature();
        final ImmutableList<IGenericType> typeParameters = typeParameters(signature.typeParameters());
        final Scope enclosingScope = scope;
        return inScope(scope.enterFunction(typeParameters), () -> {
            final IOverloadedFunctionFragment function = new IOverloadedFunctionFragment(name(signature.identifier()),
                    typeParameters, parameters(signature.parameter()), annotations);
            declareOperators(enclosingScope, annotations, enclosingScope.declareOverloadedFunction(function),
                    function.parameters().size());
            return function;
        });
    }

    /**
     * Builds a variant definition, which implements a variant with a body, as a function definition does.
     */
    @Nonnull
    private IFunctionDefinitionFragment variantDefinition(@Nonnull final FrlIrParser.VariantDefinitionContext ctx) {
        final ImmutableList<IAnnotationFragment> annotations = annotations(ctx.annotation());
        return variant(ctx.variantSignature(), ctx.type(), annotations, (signature, variant) ->
                new IFunctionDefinitionFragment(signature.name(), signature.typeParameters(), signature.parameters(),
                        signature.resultType(), functionBody(signature, ctx.block()), annotations, variant));
    }

    /**
     * Builds a variant declaration, a variant that the runtime provides. Like a function declaration, it records the
     * package of the program.
     */
    @Nonnull
    private IFunctionDeclarationFragment variantDeclaration(@Nonnull final FrlIrParser.VariantDeclarationContext ctx) {
        final ImmutableList<IAnnotationFragment> annotations = annotations(ctx.annotation());
        return variant(ctx.variantSignature(), ctx.type(), annotations, (signature, variant) ->
                new IFunctionDeclarationFragment(signature.name(), signature.typeParameters(), signature.parameters(),
                        signature.resultType(), annotations, scope.packageStatement(), variant));
    }

    /**
     * Builds a variant of an overloaded function. The overloaded function must be declared in the current scope, before
     * the variant. The variant has no type parameters of its own, so its scope declares none, and the fragment checks
     * that its signature is that of the overloaded function for its type arguments. The variant is added to the
     * overloaded function once it is built, so that its body cannot call it.
     *
     * @param resultType the result type of the variant, or {@code null} if it returns {@code void}
     * @param annotations the annotations of the variant, which cannot make it the function of an operator: the
     *        operator of the overloaded function calls its variants
     * @param builder builds the variant, in the scope of the variant, from the signature and the variant part
     */
    @Nonnull
    private <F extends IFunctionFragment> F variant(
            @Nonnull final FrlIrParser.VariantSignatureContext ctx, @Nullable final FrlIrParser.TypeContext resultType,
            @Nonnull final List<IAnnotationFragment> annotations,
            @Nonnull final BiFunction<Signature, IOverloadedFunctionFragment.Variant, F> builder) {
        checkNoOperator(annotations);
        final String name = name(ctx.identifier());
        final Scope.OverloadedFunctionBinding overloadedFunction = scope.overloadedFunction(name)
                .orElseThrow(() -> new IllegalArgumentException(
                        "a variant must be declared after its overloaded function, in the same scope"));
        final IOverloadedFunctionFragment.Variant variant = new IOverloadedFunctionFragment.Variant(
                overloadedFunction.function(), typeArguments(ctx.typeArguments()));
        final Scope enclosingScope = scope;
        return inScope(scope.enterFunction(ImmutableList.of()), () -> {
            final F function = builder.apply(new Signature(name, ImmutableList.of(), parameters(ctx.parameter()),
                    resultType == null ? voidType() : type(resultType)), variant);
            enclosingScope.declareVariant(function);
            return function;
        });
    }

    /**
     * Declares the operators that the {@code @operator} annotations of a function name. Each operator then calls the
     * function. The number of parameters of the function decides between the two operators with the symbol {@code -}:
     * a function with one parameter is the prefix minus, and a function with two is the binary minus.
     *
     * <p>Example: {@code @operator("+") overloaded func add<A, B>(a: A, b: B)} makes {@code x + y} a call
     * {@code add(x, y)}, without labels.</p>
     *
     * @param declaringScope the scope that declares the function
     * @param annotations the annotations of the function
     * @param function the function as the scope declares it
     * @param parameterCount the number of parameters of the function
     */
    private static void declareOperators(@Nonnull final Scope declaringScope,
                                         @Nonnull final List<IAnnotationFragment> annotations,
                                         @Nonnull final Scope.CalleeBinding function, final int parameterCount) {
        for (final IAnnotationFragment annotation : annotations) {
            if (OPERATOR_ANNOTATION.equals(annotation.name())) {
                final List<IAnnotationFragment.Argument> arguments = annotation.arguments();
                Preconditions.checkArgument(arguments.size() == 1 && arguments.get(0).label() == null,
                        OPERATOR_ANNOTATION_FORM);
                final Operator operator = Operator.of(arguments.get(0).value(), parameterCount)
                        .orElseThrow(() -> new IllegalArgumentException(OPERATOR_ANNOTATION_FORM));
                declaringScope.declareOperator(operator, function);
            }
        }
    }

    /**
     * Checks that annotations of something other than a function that is not a variant have no {@code @operator}
     * annotation.
     */
    private static void checkNoOperator(@Nonnull final List<IAnnotationFragment> annotations) {
        Preconditions.checkArgument(annotations.stream()
                .noneMatch(annotation -> OPERATOR_ANNOTATION.equals(annotation.name())), OPERATOR_ANNOTATION_PLACE);
    }

    /**
     * Builds a type declaration and declares the extern type in the current scope. Like a function declaration, it
     * records the package of the program.
     */
    @Nonnull
    private ITypeDeclarationFragment typeDeclaration(@Nonnull final FrlIrParser.TypeDeclarationContext ctx) {
        final ImmutableList<IAnnotationFragment> annotations = annotations(ctx.annotation());
        checkNoOperator(annotations);
        final ITypeDeclarationFragment declaration =
                new ITypeDeclarationFragment(name(ctx.identifier()), annotations, scope.packageStatement());
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
        final ImmutableList<IAnnotationFragment> annotations = annotations(declaration.annotation());
        checkNoOperator(annotations);
        final IPackageStatementFragment packageStatement =
                new IPackageStatementFragment(name(declaration.identifier()), annotations);
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

    /**
     * Creates a parameter of a function definition or a function declaration. Without a separate label, the label of
     * the parameter is its name. The label {@code _} without backticks means that the parameter has no label.
     */
    @Nonnull
    @Override
    public IFunctionDefinitionFragment.Parameter visitParameter(@Nonnull final FrlIrParser.ParameterContext ctx) {
        final FrlIrParser.IdentifierContext label = ctx.label == null ? ctx.name : ctx.label;
        return new IFunctionDefinitionFragment.Parameter(isNoLabel(label) ? null : name(label), name(ctx.name),
                type(ctx.type()));
    }

    /**
     * Returns whether an identifier is {@code _} without backticks, which means "no label" in the place of a label.
     */
    private static boolean isNoLabel(@Nonnull final FrlIrParser.IdentifierContext ctx) {
        return NO_LABEL.equals(ctx.getText());
    }

    /**
     * Returns the name of a field of a record constructor: the name that its label gives, or none if it has no label.
     * The label {@code _} needs backticks, because Swift reads {@code (_: 1, 2)} as a tuple without labels.
     */
    @Nonnull
    private static Optional<String> fieldName(@Nullable final FrlIrParser.IdentifierContext label) {
        if (label == null) {
            return Optional.empty();
        }
        Preconditions.checkArgument(!isNoLabel(label),
                "the label of a field of a record constructor cannot be _ without backticks");
        return Optional.of(name(label));
    }

    /**
     * Returns the ordinal that the digits of a field access by ordinal denote. A number beyond the range of an int is
     * the ordinal of no field.
     */
    private static int ordinal(@Nonnull final String digits) {
        try {
            return Integer.parseInt(digits);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(TypeInference.NO_SUCH_FIELD, e);
        }
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

    /**
     * Returns the types of type arguments, read in the current scope.
     */
    @Nonnull
    private ImmutableList<IType> typeArguments(@Nonnull final FrlIrParser.TypeArgumentsContext ctx) {
        return ctx.type().stream().map(this::type).collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the types of type arguments that may be absent.
     *
     * @return the types, or {@code null} if there are no type arguments
     */
    @Nullable
    private ImmutableList<IType> optionalTypeArguments(@Nullable final FrlIrParser.TypeArgumentsContext ctx) {
        return ctx == null ? null : typeArguments(ctx);
    }

    //
    // Expressions
    //

    /**
     * Infers the type of an expression: at most one comparison of operands that hold the other operators, see
     * {@link #binary}.
     */
    @Nonnull
    private Inferred infer(@Nonnull final FrlIrParser.ExpressionContext ctx,
                           @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final List<FrlIrParser.AdditiveExpressionContext> operands = ctx.additiveExpression();
        return binary(operands, comparisonOperators(ctx.comparisonOperator()), this::inferAdditive, operands.size() - 1,
                expected, mismatch);
    }

    @Nonnull
    private Inferred inferAdditive(@Nonnull final FrlIrParser.AdditiveExpressionContext ctx,
                                   @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final List<FrlIrParser.MultiplicativeExpressionContext> operands = ctx.multiplicativeExpression();
        return binary(operands, additiveOperators(ctx.additiveOperator()), this::inferMultiplicative,
                operands.size() - 1, expected, mismatch);
    }

    @Nonnull
    private Inferred inferMultiplicative(@Nonnull final FrlIrParser.MultiplicativeExpressionContext ctx,
                                         @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final List<FrlIrParser.PrefixExpressionContext> operands = ctx.prefixExpression();
        return binary(operands, multiplicativeOperators(ctx.multiplicativeOperator()), this::inferPrefix,
                operands.size() - 1, expected, mismatch);
    }

    /**
     * Infers a prefix operation, {@code -x} or {@code !b}, or a primary expression with its postfixes.
     */
    @Nonnull
    private Inferred inferPrefix(@Nonnull final FrlIrParser.PrefixExpressionContext ctx,
                                 @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final FrlIrParser.PrefixOperatorContext operator = ctx.prefixOperator();
        if (operator == null) {
            return infer(ctx.primaryExpression(), ctx.postfix(), expected, mismatch);
        }
        return operation(prefixOperator(operator), ImmutableList.of(parameterType ->
                inferPrefix(ctx.prefixExpression(), parameterType, ARGUMENT_MISMATCH)), expected, mismatch);
    }

    /**
     * Infers the type of the expression in front of the block of an if, a for, or a consume, whose calls take no
     * trailing closure. Its operators are those of an expression, see {@link #binary}.
     */
    @Nonnull
    private Inferred infer(@Nonnull final FrlIrParser.ConditionExpressionContext ctx,
                           @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final List<FrlIrParser.ConditionAdditiveExpressionContext> operands = ctx.conditionAdditiveExpression();
        return binary(operands, comparisonOperators(ctx.comparisonOperator()), this::inferConditionAdditive,
                operands.size() - 1, expected, mismatch);
    }

    @Nonnull
    private Inferred inferConditionAdditive(@Nonnull final FrlIrParser.ConditionAdditiveExpressionContext ctx,
                                            @Nullable final TypeInference.Term expected,
                                            @Nonnull final String mismatch) {
        final List<FrlIrParser.ConditionMultiplicativeExpressionContext> operands =
                ctx.conditionMultiplicativeExpression();
        return binary(operands, additiveOperators(ctx.additiveOperator()), this::inferConditionMultiplicative,
                operands.size() - 1, expected, mismatch);
    }

    @Nonnull
    private Inferred inferConditionMultiplicative(
            @Nonnull final FrlIrParser.ConditionMultiplicativeExpressionContext ctx,
            @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final List<FrlIrParser.ConditionPrefixExpressionContext> operands = ctx.conditionPrefixExpression();
        return binary(operands, multiplicativeOperators(ctx.multiplicativeOperator()), this::inferConditionPrefix,
                operands.size() - 1, expected, mismatch);
    }

    @Nonnull
    private Inferred inferConditionPrefix(@Nonnull final FrlIrParser.ConditionPrefixExpressionContext ctx,
                                          @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final FrlIrParser.PrefixOperatorContext operator = ctx.prefixOperator();
        if (operator == null) {
            return infer(ctx.primaryExpression(), ctx.conditionPostfix(), expected, mismatch);
        }
        return operation(prefixOperator(operator), ImmutableList.of(parameterType ->
                inferConditionPrefix(ctx.conditionPrefixExpression(), parameterType, ARGUMENT_MISMATCH)), expected,
                mismatch);
    }

    /**
     * Infers operands with binary operators of one level between them. The operators apply from left to right, so
     * {@code a - b - c} is {@code (a - b) - c}: the method infers the last operation as a call whose first argument is
     * the operation before it. Each operand is inferred with the type of its parameter as its context, as an argument
     * of a call is. The expected type is the context of the last operation, or of the only operand if there is no
     * operator.
     *
     * @param operands the operands, in order
     * @param operators the operators between the operands, in order, one fewer than the operands
     * @param operand infers an operand, which can hold the operators of a stronger level
     * @param last the index of the last operand that the operation includes
     * @param expected the type that the context of the operation requires, or {@code null} if it requires none
     * @param mismatch the message of the exception if the operation cannot have the expected type
     */
    @Nonnull
    private <C> Inferred binary(@Nonnull final List<C> operands, @Nonnull final List<OperatorUse> operators,
                                @Nonnull final OperandInference<C> operand, final int last,
                                @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        if (last == 0) {
            return operand.infer(operands.get(0), expected, mismatch);
        }
        return operation(operators.get(last - 1), ImmutableList.of(
                parameterType -> binary(operands, operators, operand, last - 1, parameterType, ARGUMENT_MISMATCH),
                parameterType -> operand.infer(operands.get(last), parameterType, ARGUMENT_MISMATCH)),
                expected, mismatch);
    }

    /**
     * Infers an operation, a call of the function that the operator has in scope, with the operands as the arguments
     * in order and without labels. Type arguments at the operator go to that function, as at a reference to it.
     *
     * @param operator the operator and its type arguments
     * @param operands infer the operands, each given the type of its parameter
     * @param expected the type that the context of the operation requires, or {@code null} if it requires none
     * @param mismatch the message of the exception if the operation cannot have the expected type
     */
    @Nonnull
    private Inferred operation(@Nonnull final OperatorUse operator,
                               @Nonnull final List<Function<TypeInference.Term, Inferred>> operands,
                               @Nullable final TypeInference.Term expected, @Nonnull final String mismatch) {
        final Scope.CalleeBinding function = scope.lookupOperator(operator.operator())
                .orElseThrow(() -> new IllegalArgumentException(NO_OPERATOR_FUNCTION));
        return inferCall(reference(function, optionalTypeArguments(operator.typeArguments())), operands.stream()
                .map(operand -> new CallArgument(null, false, operand))
                .collect(ImmutableList.toImmutableList()), expected, mismatch);
    }

    @Nonnull
    private static ImmutableList<OperatorUse> comparisonOperators(
            @Nullable final FrlIrParser.ComparisonOperatorContext ctx) {
        return ctx == null ? ImmutableList.of() : ImmutableList.of(new OperatorUse(binaryOperator(ctx.symbol),
                ctx.typeArguments()));
    }

    @Nonnull
    private static ImmutableList<OperatorUse> additiveOperators(
            @Nonnull final List<FrlIrParser.AdditiveOperatorContext> ctx) {
        return ctx.stream()
                .map(operator -> new OperatorUse(binaryOperator(operator.symbol), operator.typeArguments()))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static ImmutableList<OperatorUse> multiplicativeOperators(
            @Nonnull final List<FrlIrParser.MultiplicativeOperatorContext> ctx) {
        return ctx.stream()
                .map(operator -> new OperatorUse(binaryOperator(operator.symbol), operator.typeArguments()))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static OperatorUse prefixOperator(@Nonnull final FrlIrParser.PrefixOperatorContext ctx) {
        final Operator operator = ctx.symbol.getType() == FrlIrParser.MINUS ? Operator.NEGATE : Operator.NOT;
        return new OperatorUse(operator, ctx.typeArguments());
    }

    /**
     * Returns the binary operator of a token. The token type tells the operator, since a token that the lexer folds a
     * line break into has the line break in its text.
     */
    @Nonnull
    private static Operator binaryOperator(@Nonnull final Token symbol) {
        return switch (symbol.getType()) {
            case FrlIrParser.STAR -> Operator.MULTIPLY;
            case FrlIrParser.SLASH -> Operator.DIVIDE;
            case FrlIrParser.PERCENT -> Operator.REMAINDER;
            case FrlIrParser.PLUS -> Operator.ADD;
            case FrlIrParser.MINUS -> Operator.SUBTRACT;
            case FrlIrParser.LT -> Operator.LESS;
            case FrlIrParser.LE -> Operator.LESS_OR_EQUAL;
            case FrlIrParser.GT -> Operator.GREATER;
            case FrlIrParser.GE -> Operator.GREATER_OR_EQUAL;
            case FrlIrParser.EQUAL_EQUAL -> Operator.EQUAL;
            case FrlIrParser.BANG_EQUAL -> Operator.NOT_EQUAL;
            default -> throw new IllegalStateException("the grammar allows only these binary operators");
        };
    }

    /**
     * Infers the type of an expression, a primary expression followed by postfixes, each of which applies to the
     * expression before it: a call, a trailing closure, a dot call, or a field access. The expected type, if there is
     * one, is the context of the expression: the type stated for a variable, the type of a parameter, or the result
     * type of a body. It is the context of the last postfix, the one whose result the expression is.
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
     * Infers the type of a call. The method makes a new variable for each argument and one for the result. It unifies
     * the type of the callee with the function type of these variables, and the result variable with the expected
     * type. Then it infers the arguments, each with its variable as the expected type. So a closure among the
     * arguments, also a trailing closure, has the type of its parameter as its context before the visitor examines
     * its body.
     *
     * <p>Example: in {@code apply({ x in h(x) }, 42)} with {@code apply<T, U>(f: (T) -> U, x: T) -> U}, the method
     * makes {@code vP1}, {@code vP2}, and {@code vR}. It unifies {@code ((vT) -> vU, vT) -> vU} with
     * {@code (vP1, vP2) -> vR}. So the closure gets the expected type {@code vP1}, which stands for
     * {@code (vT) -> vU}, and {@code 42} gets {@code vP2}, which stands for the same type as {@code vT}.</p>
     *
     * @param arguments the arguments in order, each of which is inferred given the type of its parameter
     */
    @Nonnull
    private Inferred inferCall(@Nonnull final Inferred callee, @Nonnull final List<CallArgument> arguments,
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
            inferredArguments.add(arguments.get(i).inference().apply(parameterTypes.get(i)));
        }
        // the arguments can decide the variant of an overloaded function, which the code after the call can need
        inference.propagate();
        final ImmutableList<Inferred> builtArguments = inferredArguments.build();
        return new Inferred(resultType, () -> new IFunctionCallFragment(callee.builder().get(), builtArguments.stream()
                .map(argument -> argument.builder().get())
                .collect(ImmutableList.toImmutableList())));
    }

    /**
     * Checks the labels of the arguments of a call. A call of a declared function writes the label of each parameter
     * before its argument, and no label for a parameter without a label. A trailing closure has no label. A call of a
     * function value has no labels, because a function type has no labels.
     *
     * <p>Example: after {@code extern func range<K>(closed: K, open: K) -> range}, the call
     * {@code range(closed: a, open: b)} is correct. The calls {@code range(a, b)} and {@code range(open: b, closed: a)}
     * fail.</p>
     *
     * @param parameters the parameters of the declared function that the call names, or {@code null} if the callee is
     *        a function value
     * @param arguments the arguments that the call writes, in order
     */
    private static void checkLabels(@Nullable final List<IFunctionDefinitionFragment.Parameter> parameters,
                                    @Nonnull final List<CallArgument> arguments) {
        if (parameters == null) {
            Preconditions.checkArgument(arguments.stream().allMatch(argument -> argument.label() == null),
                    FUNCTION_VALUE_LABELS);
            return;
        }
        if (parameters.size() != arguments.size()) {
            // the unification of the callee with the arguments reports the wrong number of arguments
            return;
        }
        for (int i = 0; i < arguments.size(); i++) {
            final CallArgument argument = arguments.get(i);
            Preconditions.checkArgument(argument.isTrailingClosure()
                            || Objects.equals(argument.label(), parameters.get(i).label().orElse(null)),
                    ARGUMENT_LABELS);
        }
    }

    /**
     * Returns the arguments of a call: those between the parentheses, if there are parentheses, followed by the
     * trailing closure, if there is one. Each argument is inferred with the type of its parameter as its context, so a
     * trailing closure, like every closure, has that type before its body is inferred.
     */
    @Nonnull
    private ImmutableList<CallArgument> callArguments(@Nonnull final FrlIrParser.CallArgumentsContext ctx) {
        final ImmutableList.Builder<CallArgument> arguments = ImmutableList.builder();
        if (ctx.arguments() != null) {
            arguments.addAll(callArguments(ctx.arguments()));
        }
        final FrlIrParser.ClosureContext trailingClosure = ctx.closure();
        if (trailingClosure != null) {
            arguments.add(new CallArgument(null, true, parameterType ->
                    new ExpressionInferenceVisitor(parameterType, ARGUMENT_MISMATCH).inferClosure(trailingClosure)));
        }
        return arguments.build();
    }

    /**
     * Returns the arguments between the parentheses of a call, each with its label, if it has one. Each argument is
     * inferred with the type of its parameter as its context. A call cannot write the label {@code _} without
     * backticks: it writes no label for a parameter without a label.
     */
    @Nonnull
    private ImmutableList<CallArgument> callArguments(@Nonnull final FrlIrParser.ArgumentsContext ctx) {
        return ctx.argument()
                .stream()
                .map(argument -> {
                    Preconditions.checkArgument(argument.label == null || !isNoLabel(argument.label), ARGUMENT_LABELS);
                    return new CallArgument(argument.label == null ? null : name(argument.label), false,
                            parameterType -> infer(argument.expression(), parameterType, ARGUMENT_MISMATCH));
                })
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the type of a reference to a function or an overloaded function, see {@link #functionReference} and
     * {@link #overloadedReference}.
     *
     * @param typeArguments the type arguments that the reference gives, or {@code null} if it gives none
     */
    @Nonnull
    private Inferred reference(@Nonnull final Scope.CalleeBinding function,
                               @Nullable final List<IType> typeArguments) {
        if (function instanceof Scope.OverloadedFunctionBinding overloadedFunction) {
            return overloadedReference(overloadedFunction, typeArguments);
        }
        return functionReference((Scope.FunctionBinding)function, typeArguments);
    }

    /**
     * Returns the type of a reference to a function, in which new inference variables stand for the type parameters of
     * the function; what they are solved as becomes the type arguments of the reference. The reference refers to the
     * function that the name stands for where the reference is. Type arguments that the reference gives, as in
     * {@code id<bigint>}, fix these variables.
     *
     * @param typeArguments the type arguments that the reference gives, or {@code null} if it gives none
     */
    @Nonnull
    private Inferred functionReference(@Nonnull final Scope.FunctionBinding function,
                                       @Nullable final List<IType> typeArguments) {
        final TypeInference.Instantiation instantiation =
                inference().instantiate(function.typeParameters(), function.type());
        fixTypeArguments(instantiation, typeArguments);
        // an instantiated function type resolves to a function type
        return new Inferred(instantiation.type(), () -> new IFunctionReferenceFragment(function.function(),
                instantiation.typeArguments()
                        .stream()
                        .map(TypeInference::resolve)
                        .collect(ImmutableList.toImmutableList()),
                (IFunctionType)TypeInference.resolve(instantiation.type())), function.function().parameters());
    }

    /**
     * Returns the type of a reference to an overloaded function. Its type has new inference variables for the type
     * parameters, as for a generic function, and a new variable for the result type. The inference then chooses the
     * variant whose key and result type fit these variables, see {@code TypeInference.choose}, from the variants
     * declared before the reference. The reference refers to that variant. Type arguments that the reference gives, as
     * in {@code add<int, int>}, fix the variables before the choice.
     *
     * @param typeArguments the type arguments that the reference gives, or {@code null} if it gives none
     */
    @Nonnull
    private Inferred overloadedReference(@Nonnull final Scope.OverloadedFunctionBinding binding,
                                         @Nullable final List<IType> typeArguments) {
        final TypeInference inference = inference();
        final IOverloadedFunctionFragment function = binding.function();
        final TypeInference.Term resultType = inference.unknown();
        final TypeInference.Instantiation instantiation = inference.instantiate(function.typeParameters(),
                function.parameters()
                        .stream()
                        .map(IFunctionDefinitionFragment.Parameter::type)
                        .collect(ImmutableList.toImmutableList()), resultType);
        fixTypeArguments(instantiation, typeArguments);
        final TypeInference.Choice choice =
                inference.choose(instantiation.typeArguments(), resultType, binding.variants());
        return new Inferred(instantiation.type(), () -> {
            final IFunctionFragment variant = choice.chosen();
            return new IFunctionReferenceFragment(variant, ImmutableList.of(), variant.type());
        }, function.parameters());
    }

    /**
     * Fixes the type arguments of a reference to the types that the reference gives, if it gives any. It must give one
     * for each type parameter of the function.
     */
    private void fixTypeArguments(@Nonnull final TypeInference.Instantiation instantiation,
                                  @Nullable final List<IType> typeArguments) {
        if (typeArguments == null) {
            return;
        }
        final List<TypeInference.Term> variables = instantiation.typeArguments();
        Preconditions.checkArgument(typeArguments.size() == variables.size(), TYPE_ARGUMENTS);
        for (int i = 0; i < variables.size(); i++) {
            // a variable of a type parameter has no constraint yet
            inference().unify(variables.get(i), TypeInference.known(typeArguments.get(i)), TYPE_ARGUMENTS);
        }
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
        return recordType(ctx.recordTypeArguments());
    }

    /**
     * Returns the record type, not nullable, of the fields between angle brackets: those of a record type,
     * {@code record<id: int>}, or the type arguments of the long form of a record constructor,
     * {@code record.of<id: int>(id: 1)}.
     */
    @Nonnull
    private IRecordType recordType(@Nonnull final FrlIrParser.RecordTypeArgumentsContext ctx) {
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
     * Infers a statement, with one method for each alternative of the {@code statement} rule. Each method states the
     * equations of the statement in the inference of the current scope, and it declares the names of the statement.
     * It returns a {@link PendingStatement}: a builder for the fragment, and the term of the value if the statement is
     * an expression.
     *
     * <p>Example: for {@code let a = 42}, the visitor makes the variable {@code vL} of the literal, and it declares
     * {@code a} with the term {@code vL}. In a function body, the statement has its own inference. So {@code solve()}
     * gives {@code vL := int} before the next statement. In a closure, the statement uses the inference of the
     * statement around the closure. So a later statement {@code h(a)} with {@code h(y: bigint)} can still give
     * {@code vL := bigint}.</p>
     */
    private final class StatementInferenceVisitor extends FrlIrParserBaseVisitor<PendingStatement> {
        /**
         * The expected type of the value of the statement, if the statement is the last one of a body whose value has
         * an expected type. Example: in {@code func f() -> bigint { 42 }}, the statement {@code 42} has the expected
         * type {@code bigint}.
         */
        @Nullable
        private final TypeInference.Term expectedValue;
        /**
         * Whether the statement has its own inference, as a statement of a program or of a function body has. If it
         * has, the statements of the blocks of an if or a for also get their own inferences. So a statement in such a
         * block gets the same types as at the place of the if or the for.
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
        public PendingStatement visitOverloadedFunctionStatement(
                @Nonnull final FrlIrParser.OverloadedFunctionStatementContext ctx) {
            return PendingStatement.built(overloadedFunction(ctx.overloadedFunctionDeclaration()));
        }

        @Nonnull
        @Override
        public PendingStatement visitVariantDefinitionStatement(
                @Nonnull final FrlIrParser.VariantDefinitionStatementContext ctx) {
            return PendingStatement.built(variantDefinition(ctx.variantDefinition()));
        }

        @Nonnull
        @Override
        public PendingStatement visitVariantDeclarationStatement(
                @Nonnull final FrlIrParser.VariantDeclarationStatementContext ctx) {
            return PendingStatement.built(variantDeclaration(ctx.variantDeclaration()));
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

        /**
         * Infers a return, which leaves the function or the closure whose body it is in, see
         * {@code Scope.returnTarget()}. The result type of that function or closure is the context of the value. A
         * return without a value requires the result type to be {@code void}. In a closure whose result type is not
         * known yet, this makes it {@code void}, and a return with a value makes it the type of the value.
         */
        @Nonnull
        @Override
        public PendingStatement visitReturnStatement(@Nonnull final FrlIrParser.ReturnStatementContext ctx) {
            final Scope.ReturnTarget target = scope.returnTarget();
            if (target == null) {
                throw new IllegalArgumentException(RETURN_OUTSIDE_BODY);
            }
            final FrlIrParser.ExpressionContext value = ctx.returnValue().expression();
            if (value == null) {
                inference().unify(target.resultType(), TypeInference.known(voidType()),
                        "a return needs a value unless the result type of its function or closure is void");
                return PendingStatement.built(new IReturnFragment(null));
            }
            Preconditions.checkArgument(!TypeInference.isResolved(target.resultType())
                            || !TypeInference.resolve(target.resultType()).isVoid(),
                    "a return has no value if the result type of its function or closure is void");
            target.addValueReturn();
            final Inferred inferredValue = infer(value, target.resultType(),
                    "the value of a return must be of the result type of its function or closure");
            return new PendingStatement(() -> new IReturnFragment(inferredValue.builder().get()), null);
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
         * Infers a for loop. The sequence is an array, and the element type of the array is the type of the loop
         * variable. The visitor declares the variable after it infers the sequence, so the sequence cannot refer to
         * it. The variable is in a scope of the loop, and the body is in a scope nested in it, as for the parameters
         * of a closure.
         *
         * <p>If the loop has its own inference, the statements of the body get their own inferences too. Then the
         * visitor calls {@code solve()} for the loop before it infers the body, and the variable gets a {@code Known}
         * term. Example: with {@code numbers<T>(x: T) -> [T]}, the loop {@code for x in numbers(1) { h(x) }} in a
         * function body makes {@code x} an int, so {@code h(y: bigint)} fails. In a closure, the body uses the
         * inference of the loop, and {@code h(x)} makes {@code x} a bigint.</p>
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
     * Infers a primary expression, with one method for each alternative of the {@code primaryExpression} and the
     * {@code literal} rules. The result is an {@link Inferred}: the term of the expression, and a builder for its
     * fragment. If the context requires a type, the visitor unifies the term with it, see {@link #fit}.
     *
     * <p>Example: the literal {@code 42} gets a new variable {@code vL} whose constraint permits only numeric types. In
     * {@code f(42)} with {@code f(x: bigint)}, the expected type of {@code 42} stands for {@code bigint}. So
     * {@code fit} gives {@code vL := bigint}, and after {@code solve()} the builder makes the literal {@code 42L}.</p>
     */
    private final class ExpressionInferenceVisitor extends FrlIrParserBaseVisitor<Inferred> {
        /**
         * The type that the context of the expression requires, if any. Examples: the type of a parameter, the type
         * stated for a variable, and the result type of a body.
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
         * Infers the type of a name. For a function, the method makes a reference with a new variable for each type
         * parameter, see {@code TypeInference.instantiate}; type arguments after the name fix these variables. For an
         * overloaded function, the reference refers to the variant that the inference chooses. For a variable, the
         * method uses the term of the binding of the variable in the scope. The owner of the binding decides how:
         * <ul>
         *     <li>If the binding has no owner, or if the owner is the current inference, the reference uses the term
         *     itself. Later equations of the statement can then still bind the variables in the term.</li>
         *     <li>If the owner is another inference, the term must be resolved, and the reference uses a copy of its
         *     type. So the current inference cannot bind a variable of another inference.</li>
         * </ul>
         * Example: in a function body, {@code let a = 42} and {@code let b = a} have different inferences. So
         * {@code b} gets a copy of the type of {@code a}, which is {@code int}. A function nested in a closure can use
         * a variable of the closure only if the type of the variable is known already.
         */
        @Nonnull
        @Override
        public Inferred visitNameExpression(@Nonnull final FrlIrParser.NameExpressionContext ctx) {
            final TypeInference inference = inference();
            final String name = name(ctx.identifier());
            final Scope.Binding binding = scope.lookupValue(name)
                    .orElseThrow(() -> new IllegalArgumentException("unknown name"));
            if (binding instanceof Scope.VariableBinding variable) {
                Preconditions.checkArgument(ctx.typeArguments() == null, TYPE_ARGUMENTS);
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
            return fit(reference((Scope.CalleeBinding)binding, optionalTypeArguments(ctx.typeArguments())));
        }

        @Nonnull
        @Override
        public Inferred visitClosureExpression(@Nonnull final FrlIrParser.ClosureExpressionContext ctx) {
            return inferClosure(ctx.closure());
        }

        /**
         * Infers a parenthesized expression as the expression in the parentheses. The parentheses only group, so the
         * context of the parentheses is the context of that expression, and a name in them keeps the labels of its
         * function: {@code (f)(x: 1)} needs the label of {@code f}.
         */
        @Nonnull
        @Override
        public Inferred visitParenthesizedExpression(@Nonnull final FrlIrParser.ParenthesizedExpressionContext ctx) {
            return infer(ctx.expression(), expected, mismatch);
        }

        /**
         * Infers the type of a record constructor: the short form, {@code (id: 1, name: "a")}, or the long form,
         * {@code record.of(id: 1, name: "a")}. Both forms give the same fragment. Only the long form can have no fields
         * or one field without a name, and only the long form takes type arguments, which state the fields:
         * {@code record.of<id(1): bigint>(id: 5)}.
         * <ul>
         *     <li>Without type arguments, each field gets a new variable for its type, and the label of the field, if
         *     it has one, as its name.</li>
         *     <li>With type arguments, each field has the type, the name, and the number that they state. The labels
         *     follow the rule of a call: a value has the name of its field as its label, and no label if the field has
         *     none.</li>
         * </ul>
         * The type of the constructor is the term of a record constructor over these fields, which can still become
         * nullable, see {@code TypeInference.recordConstructor}. The method unifies this type with the expected type
         * before it infers the values. So the context gives each value its type, as the type of a parameter does to an
         * argument, and the context decides whether the record is nullable.
         *
         * <p>Example: in {@code let k: record<bigint, string?> = (10, nil)}, the fields get {@code vA} and {@code vB},
         * and the constructor gets {@code vK}, which stands for {@code record<vA, vB>}, nullable or not. The expected
         * type gives {@code vK := record<bigint, string?>}, so {@code vA := bigint} and {@code vB := string?}. Then
         * {@code 10} becomes a bigint, and {@code nil} a nullable string.</p>
         */
        @Nonnull
        @Override
        public Inferred visitRecordExpression(@Nonnull final FrlIrParser.RecordExpressionContext ctx) {
            final TypeInference inference = inference();
            final FrlIrParser.RecordConstructorContext record = ctx.recordConstructor();
            Preconditions.checkArgument(record.member == null || RECORD_CONSTRUCTOR_MEMBER.equals(name(record.member)),
                    "the long form of a record constructor is record.of");
            final List<Optional<String>> labels;
            final List<FrlIrParser.ExpressionContext> values;
            if (record.label != null) {
                // the short form of a record of one field, which has a label
                labels = ImmutableList.of(fieldName(record.label));
                values = ImmutableList.of(record.expression());
            } else {
                // the short form of two fields or more, or the long form of any number of fields
                labels = record.recordConstructorField()
                        .stream()
                        .map(field -> fieldName(field.label))
                        .collect(ImmutableList.toImmutableList());
                values = record.recordConstructorField()
                        .stream()
                        .map(FrlIrParser.RecordConstructorFieldContext::expression)
                        .collect(ImmutableList.toImmutableList());
            }
            final List<String> presentLabels =
                    labels.stream().flatMap(Optional::stream).collect(ImmutableList.toImmutableList());
            Preconditions.checkArgument(presentLabels.stream().distinct().count() == presentLabels.size(),
                    "the fields of a record must have distinct names");
            final IRecordType stated = record.recordTypeArguments() == null
                    ? null
                    : recordType(record.recordTypeArguments());
            final ImmutableList<TypeInference.Term> fieldTypes;
            final TypeInference.Term type;
            if (stated == null) {
                fieldTypes = values.stream()
                        .map(value -> inference.unknown())
                        .collect(ImmutableList.toImmutableList());
                type = inference.recordConstructor(fieldTypes, labels);
            } else {
                // the type arguments are the fields, and a value has the label of its field, as an argument has the
                // label of its parameter
                Preconditions.checkArgument(stated.fields().size() == values.size(), RECORD_CONSTRUCTOR_VALUES);
                for (int i = 0; i < values.size(); i++) {
                    Preconditions.checkArgument(labels.get(i).equals(stated.fields().get(i).name()),
                            RECORD_CONSTRUCTOR_LABELS);
                }
                fieldTypes = stated.fields()
                        .stream()
                        .map(field -> TypeInference.known(field.type()))
                        .collect(ImmutableList.toImmutableList());
                type = inference.recordConstructor(stated);
            }
            if (expected != null) {
                inference.unify(type, expected, mismatch);
            }
            // with type arguments, a value that does not fit is wrong with respect to them, not to the context
            final String valueMismatch = stated == null ? mismatch : RECORD_CONSTRUCTOR_VALUE_MISMATCH;
            final ImmutableList.Builder<Inferred> inferredValues = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                inferredValues.add(infer(values.get(i), fieldTypes.get(i), valueMismatch));
            }
            final ImmutableList<Inferred> builtValues = inferredValues.build();
            return new Inferred(type, () -> {
                final ImmutableList.Builder<IRecordConstructorFragment.Field> fields = ImmutableList.builder();
                for (int i = 0; i < builtValues.size(); i++) {
                    fields.add(new IRecordConstructorFragment.Field(builtValues.get(i).builder().get(), labels.get(i),
                            stated == null ? Optional.empty() : stated.fields().get(i).number()));
                }
                return new IRecordConstructorFragment(TypeInference.resolve(type).isNullable(), fields.build());
            });
        }

        /**
         * Infers the type of a stream block, {@code stream { … }}, a stream of a new inference variable. The stream
         * type is unified with the expected type before the block is inferred, so that the context determines the
         * element type and every yield has it as the context of its value; without a context, the yielded values
         * determine it. The scope of the block inherits the inference of the statement that the stream block is in, so
         * the statements of the block join it, as those of a closure do. The block has no value, so a single expression
         * in it has no context, and its scope has no target of returns, since a yield break ends a stream block.
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
         * Infers the type of a closure, also of a trailing closure. A parameter or a result without a stated type gets
         * a new variable. The method unifies the function type of the closure with the expected type before it
         * infers the body. So the context fixes the types that the closure leaves out, and the body sees them. The
         * closure then needs no {@link #fit}. The scope of the closure uses the inference of the statement that the
         * closure is in, so the statements of the body use it too. The scope has no element type of yields, because a
         * closure cannot yield, and a return in the body leaves the closure. If neither the closure nor the context
         * states the result type, the result type is the type of the single expression of the body, or the type of the
         * values of its returns, or {@code void} if the body has neither.
         *
         * <p>Example: the closure {@code { x in h(x) }} with the expected type {@code (vT) -> vU} gets the term
         * {@code (vX) -> vC}. The method unifies the two, which gives {@code vX := vT} and {@code vC := vU}. Then the
         * body {@code h(x)} with {@code h(y: bigint) -> string} gives {@code vT := bigint} and
         * {@code vU := string}.</p>
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
                final Scope.ReturnTarget target = new Scope.ReturnTarget(resultType);
                final PendingStatements body = inScope(scope.enterBody(target),
                        () -> statements(closure.statements(), discardsValue ? null : resultType));
                if (statedResultType == null && body.valueType() == null && !target.returnsValue()) {
                    // a body that is not a single expression and returns no value returns void, as in Swift
                    inference.unify(resultType, TypeInference.known(voidType()),
                            "a body of a closure that is not a single expression gives its value with a return");
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
     * Infers a postfix of an expression, with one method for each alternative of the {@code postfix} and the
     * {@code conditionPostfix} rules. A postfix is one of three things:
     * <ul>
     *     <li>a call of the expression before it;</li>
     *     <li>a dot call of a function with the expression before it as the first argument;</li>
     *     <li>a field access of the record before it, by name or by ordinal.</li>
     * </ul>
     * A trailing closure is the last argument of a call, except in the expression in front of a block. Each call goes
     * through {@link FrlIrFragmentVisitor#inferCall}, so each argument has the type of its parameter as its expected
     * type.
     *
     * <p>Example: in {@code xs.map { x in x }} with {@code map<T, U>(xs: [T], f: (T) -> U) -> [U]} and
     * {@code xs: [int]}, the dot call unifies the type of {@code xs} with the first parameter, which gives
     * {@code vT := int}. The closure then has an expected type that stands for {@code (int) -> vU}. So its parameter
     * {@code x} is an int, and its body gives {@code vU := int}.</p>
     */
    private final class PostfixInferenceVisitor extends FrlIrParserBaseVisitor<Inferred> {
        /**
         * The expression that the postfix applies to.
         */
        @Nonnull
        private final Inferred before;
        /**
         * The type that the context of the postfix requires, if any.
         */
        @Nullable
        private final TypeInference.Term expected;
        /**
         * The message of the exception if the postfix cannot have the expected type.
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
            return call(callArguments(ctx.callArguments()));
        }

        @Nonnull
        @Override
        public Inferred visitDotCallPostfix(@Nonnull final FrlIrParser.DotCallPostfixContext ctx) {
            return dotCall(ctx.identifier(), ctx.typeArguments(), callArguments(ctx.callArguments()));
        }

        @Nonnull
        @Override
        public Inferred visitFieldPostfix(@Nonnull final FrlIrParser.FieldPostfixContext ctx) {
            return fieldByName(ctx.identifier());
        }

        @Nonnull
        @Override
        public Inferred visitOrdinalPostfix(@Nonnull final FrlIrParser.OrdinalPostfixContext ctx) {
            return fieldAccess(before, ordinal(ctx.INTEGER_LITERAL().getText()), expected);
        }

        @Nonnull
        @Override
        public Inferred visitOrdinalPairPostfix(@Nonnull final FrlIrParser.OrdinalPairPostfixContext ctx) {
            return fieldsByOrdinals(ctx.FLOATING_LITERAL().getText());
        }

        @Nonnull
        @Override
        public Inferred visitConditionCallPostfix(@Nonnull final FrlIrParser.ConditionCallPostfixContext ctx) {
            return call(callArguments(ctx.arguments()));
        }

        @Nonnull
        @Override
        public Inferred visitConditionDotCallPostfix(@Nonnull final FrlIrParser.ConditionDotCallPostfixContext ctx) {
            return dotCall(ctx.identifier(), ctx.typeArguments(), callArguments(ctx.arguments()));
        }

        @Nonnull
        @Override
        public Inferred visitConditionFieldPostfix(@Nonnull final FrlIrParser.ConditionFieldPostfixContext ctx) {
            return fieldByName(ctx.identifier());
        }

        @Nonnull
        @Override
        public Inferred visitConditionOrdinalPostfix(@Nonnull final FrlIrParser.ConditionOrdinalPostfixContext ctx) {
            return fieldAccess(before, ordinal(ctx.INTEGER_LITERAL().getText()), expected);
        }

        @Nonnull
        @Override
        public Inferred visitConditionOrdinalPairPostfix(
                @Nonnull final FrlIrParser.ConditionOrdinalPairPostfixContext ctx) {
            return fieldsByOrdinals(ctx.FLOATING_LITERAL().getText());
        }

        /**
         * Infers a field access by name, {@code x.name}. The name gives the ordinal of the field in the record type of
         * {@code x}, and the fragment holds only that ordinal.
         */
        @Nonnull
        private Inferred fieldByName(@Nonnull final FrlIrParser.IdentifierContext name) {
            return fieldAccess(before, inference().fieldOrdinal(before.type(), name(name)), expected);
        }

        /**
         * Infers two field accesses by ordinal that the lexer reads as one floating-point literal: {@code x.0.1} is
         * the field at ordinal 1 of the field at ordinal 0. Only a literal of two whole numbers with a point between
         * them is such a pair; a literal with an exponent is not.
         */
        @Nonnull
        private Inferred fieldsByOrdinals(@Nonnull final String text) {
            Preconditions.checkArgument(ORDINAL_PAIR.matcher(text).matches(), TypeInference.NO_SUCH_FIELD);
            final int point = text.indexOf('.');
            final Inferred first = fieldAccess(before, ordinal(text.substring(0, point)), null);
            return fieldAccess(first, ordinal(text.substring(point + 1)), expected);
        }

        /**
         * Infers an access to the field at an ordinal of a record. The type of the access is the term of the type of
         * the field, which the record type of the record gives, see {@code TypeInference.fieldType}. If the context
         * requires a type, the access must have it.
         *
         * @param record the record whose field the access reads
         * @param ordinal the position of the field in the record type
         * @param expectedType the type that the context of the access requires, or {@code null} if it requires none
         */
        @Nonnull
        private Inferred fieldAccess(@Nonnull final Inferred record, final int ordinal,
                                     @Nullable final TypeInference.Term expectedType) {
            final TypeInference.Term type = inference().fieldType(record.type(), ordinal);
            if (expectedType != null) {
                inference().unify(type, expectedType, mismatch);
            }
            return new Inferred(type, () -> new IFieldAccessFragment(record.builder().get(), ordinal));
        }

        /**
         * Infers a call of the expression before the postfix. If that expression names a declared function, also an
         * overloaded one, the arguments have the labels of its parameters. Otherwise, the callee is a function value,
         * and the arguments have no labels.
         */
        @Nonnull
        private Inferred call(@Nonnull final List<CallArgument> arguments) {
            checkLabels(before.parameters(), arguments);
            return inferCall(before, arguments, expected, mismatch);
        }

        /**
         * Infers a dot call, {@code x.f(1)}, which calls the function {@code f} with {@code x} as its first argument. The
         * name is looked up among the functions only, so that a variable of the name does not hide the function.
         * {@code x} is inferred before the function is known, so its type gets no context from the function's first
         * parameter until it is unified with that parameter. The first argument has no label, also if the first
         * parameter has a label. The other arguments have the labels of the other parameters.
         *
         * @param typeArguments the type arguments after the name of the function, or {@code null} if there are none
         * @param arguments the arguments after the first one, in order
         */
        @Nonnull
        private Inferred dotCall(@Nonnull final FrlIrParser.IdentifierContext function,
                                 @Nullable final FrlIrParser.TypeArgumentsContext typeArguments,
                                 @Nonnull final List<CallArgument> arguments) {
            final Scope.CalleeBinding binding = scope.lookupFunction(name(function))
                    .orElseThrow(() -> new IllegalArgumentException("unknown function"));
            final Inferred callee = reference(binding, optionalTypeArguments(typeArguments));
            // a reference to a declared function has the parameters of the function
            final List<IFunctionDefinitionFragment.Parameter> parameters = Objects.requireNonNull(callee.parameters());
            checkLabels(parameters.isEmpty() ? parameters : parameters.subList(1, parameters.size()), arguments);
            final CallArgument receiver = new CallArgument(null, false, parameterType -> {
                inference().unify(before.type(), parameterType, ARGUMENT_MISMATCH);
                return before;
            });
            return inferCall(callee, ImmutableList.<CallArgument>builder()
                    .add(receiver)
                    .addAll(arguments)
                    .build(), expected, mismatch);
        }
    }

    /**
     * Returns the parameters of a closure, with one method for each alternative of the {@code closureParameterClause}
     * rule. A parameter with a stated type gets a {@code Known} term. A parameter without a stated type gets a new
     * variable, which the context of the closure or its body fixes later. Example: in {@code { (x, y: int) in x }},
     * {@code x} gets a new variable {@code vX}, and {@code y} gets the term {@code known(int)}.
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
     * An expression during inference: the term of its type, and a builder for its fragment. The builder calls
     * {@code resolve} on its terms, so call it only after {@code solve()} ran for the inference of the statement.
     * Example: the literal {@code 42} gives the term {@code vL}, and a builder that makes an {@code ILiteralFragment}
     * with the type that {@code vL} resolves to.
     *
     * @param type the term of the type of the expression
     * @param builder builds the expression; call it only after the inference is solved
     * @param parameters the parameters of the declared function that the expression names, also of an overloaded
     *        one, or {@code null} if the expression does not name one. A call of the expression checks its arguments
     *        against the labels of these parameters.
     */
    private record Inferred(@Nonnull TypeInference.Term type, @Nonnull Supplier<IExpressionFragment> builder,
                            @Nullable List<IFunctionDefinitionFragment.Parameter> parameters) {
        /**
         * Creates an expression that does not name a declared function.
         *
         * @param type the term of the type of the expression
         * @param builder builds the expression; call it only after the inference is solved
         */
        private Inferred(@Nonnull final TypeInference.Term type, @Nonnull final Supplier<IExpressionFragment> builder) {
            this(type, builder, null);
        }
    }

    /**
     * An operator in an expression, with the type arguments that it gives to its function.
     *
     * @param operator the operator
     * @param typeArguments the type arguments after the operator, as in {@code a +<int, int> b}, or {@code null} if
     *        it has none
     */
    private record OperatorUse(@Nonnull Operator operator, @Nullable FrlIrParser.TypeArgumentsContext typeArguments) {
    }

    /**
     * Infers an operand of an operator, with the type of its parameter as its context.
     *
     * @param <C> the kind of operand in the grammar
     */
    @FunctionalInterface
    private interface OperandInference<C> {
        /**
         * Infers an operand.
         *
         * @param operand the operand
         * @param expected the type that the context of the operand requires, or {@code null} if it requires none
         * @param mismatch the message of the exception if the operand cannot have the expected type
         * @return the inferred operand
         */
        @Nonnull
        Inferred infer(@Nonnull C operand, @Nullable TypeInference.Term expected, @Nonnull String mismatch);
    }

    /**
     * An argument of a call during inference: the label that the call writes before the argument, and the function
     * that infers the argument for the type of its parameter.
     *
     * @param label the label before the argument, or {@code null} if the call writes none
     * @param isTrailingClosure whether the argument is a trailing closure, which has no label
     * @param inference infers the argument, given the term of the type of its parameter
     */
    private record CallArgument(@Nullable String label, boolean isTrailingClosure,
                                @Nonnull Function<TypeInference.Term, Inferred> inference) {
    }

    /**
     * A statement during inference: a builder for its fragment, and the term of its value. Only an expression
     * statement has a value. If the statement is the only one of a body, its value is the value of the body.
     *
     * @param builder builds the statement; call it only after the inference is solved
     * @param valueType the term of the value of the statement if the statement is an expression, and {@code null}
     *        otherwise
     */
    private record PendingStatement(@Nonnull Supplier<IStatementFragment> builder,
                                    @Nullable TypeInference.Term valueType) {
        /**
         * Returns a statement that is built already. Such a statement has no terms and no value: a definition, a
         * declaration, a type alias, a yield break without a reason, or a return without a value.
         */
        @Nonnull
        static PendingStatement built(@Nonnull final IStatementFragment statement) {
            return new PendingStatement(() -> statement, null);
        }
    }

    /**
     * Statements during inference: one builder for all of them, and the term of their value. As in Swift, the value is
     * the value of the only statement if there is exactly one and it is an expression. Example: the body of
     * {@code { x in h(x) }} has the value term of {@code h(x)}, the result type of the call.
     *
     * @param builder builds the statements; call it only after the inference is solved
     * @param valueType the term of the value of the statements, the value of the only one if it is an expression, and
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
     * A parameter of a closure during inference.
     *
     * @param name the name of the parameter
     * @param type the term of the type of the parameter: a {@code Known} term if the closure states the type, and a
     *        new variable otherwise
     */
    private record ClosureParameter(@Nonnull String name, @Nonnull TypeInference.Term type) {
    }
}
