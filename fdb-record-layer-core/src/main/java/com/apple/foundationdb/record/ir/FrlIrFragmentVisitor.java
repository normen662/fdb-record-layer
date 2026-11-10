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
import com.apple.foundationdb.record.ir.fragment.IFragment;
import com.apple.foundationdb.record.ir.generated.FrlIrParser;
import com.apple.foundationdb.record.ir.generated.FrlIrParserBaseVisitor;
import com.apple.foundationdb.record.ir.program.IBlock;
import com.apple.foundationdb.record.ir.program.IClosure;
import com.apple.foundationdb.record.ir.program.IFunctionDefinition;
import com.apple.foundationdb.record.ir.program.IProgram;
import com.apple.foundationdb.record.ir.program.IStatement;
import com.apple.foundationdb.record.ir.types.IArrayType;
import com.apple.foundationdb.record.ir.types.IEnumType;
import com.apple.foundationdb.record.ir.types.IFunctionType;
import com.apple.foundationdb.record.ir.types.IGenericType;
import com.apple.foundationdb.record.ir.types.IPrimitiveType;
import com.apple.foundationdb.record.ir.types.IRecordType;
import com.apple.foundationdb.record.ir.types.IStreamType;
import com.apple.foundationdb.record.ir.types.IType;
import com.apple.foundationdb.record.ir.types.IVectorType;
import com.apple.foundationdb.record.ir.types.ParserHelpers;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Builds the fragments of an FRL-IR program from its parse tree: every visit method returns the fragment of the
 * construct it visits, an {@link IProgram} for a program, an {@link IBlock} for a block, an {@link IType} for a type,
 * and so on.
 *
 * <p>The visitor keeps the {@link Scope} of the construct it is in. A function definition declares its name in the
 * enclosing scope and its type parameters in a scope of its own, a block opens a scope for what its statements declare,
 * and leaving a construct returns to the enclosing scope. A name in a type is a type only if it is a type parameter in
 * scope, so a misspelled type is an error. A visitor holds the scope of one traversal, so each parse uses a new
 * visitor.</p>
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
        return new IProgram(statements(ctx.statements()));
    }

    @Nonnull
    @Override
    public IBlock visitBlock(@Nonnull final FrlIrParser.BlockContext ctx) {
        return inNewScope(ImmutableList.of(), () -> new IBlock(statements(ctx.statements())));
    }

    /**
     * Builds the statements of a program or a block. Programs and blocks share the rule for their statements, and so
     * the code that builds them.
     */
    @Nonnull
    private ImmutableList<IStatement> statements(@Nonnull final FrlIrParser.StatementsContext ctx) {
        return ctx.statement()
                .stream()
                .map(statement -> (IStatement)visit(statement))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Builds a function definition. Its name is declared in the enclosing scope, so that its body and the statements
     * after it see the function. Its type parameters are declared in a scope of their own, which the signature and the
     * body share.
     */
    @Nonnull
    @Override
    public IFunctionDefinition visitFunctionDefinition(@Nonnull final FrlIrParser.FunctionDefinitionContext ctx) {
        final String name = name(ctx.identifier());
        scope.declareFunction(name);
        final ImmutableList<IGenericType> typeParameters = ctx.typeParameters() == null
                ? ImmutableList.of()
                : ctx.typeParameters().identifier().stream()
                        .map(identifier -> new IGenericType(name(identifier), false))
                        .collect(ImmutableList.toImmutableList());
        return inNewScope(typeParameters, () -> {
            final ImmutableList<IFunctionDefinition.Parameter> parameters = ctx.parameter()
                    .stream()
                    .map(this::visitParameter)
                    .collect(ImmutableList.toImmutableList());
            final IType resultType = ctx.type() == null
                    ? new IPrimitiveType(IPrimitiveType.Kind.VOID, false)
                    : type(ctx.type());
            return new IFunctionDefinition(name, typeParameters, parameters, resultType, visitBlock(ctx.block()));
        });
    }

    @Nonnull
    @Override
    public IFunctionDefinition.Parameter visitParameter(@Nonnull final FrlIrParser.ParameterContext ctx) {
        return new IFunctionDefinition.Parameter(name(ctx.identifier()), type(ctx.type()));
    }

    /**
     * Builds a closure, whose signature and statements share a scope of their own. No statement contains a closure yet,
     * so no program reaches this so far.
     */
    @Nonnull
    @Override
    public IClosure visitClosure(@Nonnull final FrlIrParser.ClosureContext ctx) {
        return inNewScope(ImmutableList.of(), () -> {
            final FrlIrParser.ClosureSignatureContext signature = ctx.closureSignature();
            final ImmutableList<IClosure.Parameter> parameters = signature == null
                    ? ImmutableList.of()
                    : closureParameters(signature.closureParameterClause());
            final Optional<IType> resultType = signature == null
                    ? Optional.empty()
                    : Optional.ofNullable(signature.type()).map(this::type);
            return new IClosure(parameters, resultType, new IBlock(statements(ctx.statements())));
        });
    }

    @Nonnull
    private ImmutableList<IClosure.Parameter> closureParameters(
            @Nonnull final FrlIrParser.ClosureParameterClauseContext ctx) {
        if (ctx instanceof FrlIrParser.ParenthesizedClosureParametersContext clause) {
            return clause.closureParameter()
                    .stream()
                    .map(this::visitClosureParameter)
                    .collect(ImmutableList.toImmutableList());
        }
        return ((FrlIrParser.BareClosureParametersContext)ctx).identifier()
                .stream()
                .map(identifier -> new IClosure.Parameter(name(identifier), Optional.empty()))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public IClosure.Parameter visitClosureParameter(@Nonnull final FrlIrParser.ClosureParameterContext ctx) {
        return new IClosure.Parameter(name(ctx.identifier()), Optional.ofNullable(ctx.type()).map(this::type));
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
        return new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
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
        return new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
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

    @Nonnull
    @Override
    public IVectorType visitVectorType(@Nonnull final FrlIrParser.VectorTypeContext ctx) {
        return new IVectorType(false, visitVectorElementType(ctx.vectorElementType()).kind(),
                Integer.parseInt(ctx.INTEGER_LITERAL().getText()));
    }

    @Nonnull
    @Override
    public IPrimitiveType visitVectorElementType(@Nonnull final FrlIrParser.VectorElementTypeContext ctx) {
        if (ctx.HALF() != null) {
            return new IPrimitiveType(IPrimitiveType.Kind.HALF, false);
        }
        if (ctx.FLOAT() != null) {
            return new IPrimitiveType(IPrimitiveType.Kind.FLOAT, false);
        }
        // the grammar leaves `double` as the only remaining element type
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
     * Builds a fragment in a new scope that declares the given type parameters, and returns to the current scope
     * afterwards, also if building fails.
     */
    @Nonnull
    private <F extends IFragment> F inNewScope(@Nonnull final Collection<IGenericType> typeParameters,
                                               @Nonnull final Supplier<F> fragmentBuilder) {
        scope = scope.enter(typeParameters);
        try {
            return fragmentBuilder.get();
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
}
