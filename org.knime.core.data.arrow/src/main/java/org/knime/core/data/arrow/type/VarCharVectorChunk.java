package org.knime.core.data.arrow.type;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.type.StringChunk;

public class VarCharVectorChunk extends AbstractFieldVectorChunk<VarCharVector> implements StringChunk {

	private final CharsetDecoder DECODER = Charset.forName("UTF-8").newDecoder()
			.onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
	private final CharsetEncoder ENCODER = Charset.forName("UTF-8").newEncoder()
			.onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

	public VarCharVectorChunk(BufferAllocator allocator) {
		super(allocator);
	}

	public VarCharVectorChunk(VarCharVector vector) {
		super(vector);
	}

	@Override
	public String getString(int index) {
		try {
			return DECODER.decode(ByteBuffer.wrap(m_vector.get(index))).toString();
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setString(int index, String value) {
		try {
			final ByteBuffer encode = ENCODER.encode(CharBuffer.wrap(value.toCharArray()));
			m_vector.set(index, encode.array(), 0, encode.limit());
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setMissing(int index) {
		m_vector.setNull(index);
	}

	@Override
	protected VarCharVector create(BufferAllocator allocator) {
		final VarCharVector vector = new VarCharVector("VarCharVector", allocator);
		return vector;
	}

	@Override
	public void allocateNew(int chunkSize) {
		m_vector.allocateNew(64l * chunkSize, chunkSize);
	}

}
