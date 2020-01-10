package pgdp.stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Florian Schmidt (ge75vob)
 * fs.schmidt@tum.de
 * <p>
 * Do not push this class, as importing from jupiter causes compilation failure on Artemis.
 * --> v1.6
 */
class StreamTest {
	/**
	 * Number of intermediate operations randomly applied to the stream.
	 * <p>
	 * > 15 or so is problematic as some map and filters are mutually exclusive and result in empty streams with a
	 * higher probability
	 */
	static final int NUMBER_INTER_OPS = 12;

	/**
	 * The size of the random integer stream used in the tests
	 */
	static final long RDM_STREAM_SIZE = 20_000L;

	static Method[] ourInterOps;
	static Method[] javaInterOps;
	static Object[][] interArgs;

	static Method[] ourTermOps;
	static Method[] javaTermOps;
	static Object[][] termArgs;

	static Random rdm;

	@BeforeAll
	static void init() throws NoSuchMethodException {
		ourInterOps = new Method[]{
				Stream.class.getDeclaredMethod("map", Function.class),
				Stream.class.getDeclaredMethod("filter", Predicate.class),
				Stream.class.getDeclaredMethod("distinct")
				//these don't exist in java.util.stream.Stream
				//Stream.class.getDeclaredMethod("mapChecked", ThrowingFunction.class),
				//Stream.class.getDeclaredMethod("filterChecked", ThrowingPredicate.class),
		};

		javaInterOps = new Method[]{
				java.util.stream.Stream.class.getDeclaredMethod("map", Function.class),
				java.util.stream.Stream.class.getDeclaredMethod("filter", Predicate.class),
				java.util.stream.Stream.class.getDeclaredMethod("distinct")
		};

		//the arguments used for the map and filter operations
		interArgs = new Object[][]{
				new Object[]{
						(Function<Integer, Integer>) i -> 2 * i,
						(Function<Integer, Integer>) i -> i - 2 * i + 13,
						(Function<Integer, Integer>) i -> 2048,
						(Function<Integer, Integer>) i -> i + i % 2,
						(Function<Integer, Integer>) i -> 13 * i * i - 26 * i + 213,
						(Function<Integer, Integer>) i -> -i,
				},
				new Object[]{
						(Predicate<Integer>) i -> i % 2 == 0,
						(Predicate<Integer>) i -> (i - 3) % 5 != 2,
						(Predicate<Integer>) i -> true,
						(Predicate<Integer>) i -> (i % 9) != 4
				},
				new Object[0]
		};

		ourTermOps = new Method[]{
				Stream.class.getDeclaredMethod("count"),
				Stream.class.getDeclaredMethod("findFirst"),
				Stream.class.getDeclaredMethod("reduce", BinaryOperator.class),
				//these don't exist in java.util.stream.Stream
				//Stream.class.getDeclaredMethod("toCollection", Supplier.class),
				//Stream.class.getDeclaredMethod("onErrorMap", Function.class),
				//Stream.class.getDeclaredMethod("onErrorMapChecked", ThrowingFunction.class),
				//Stream.class.getDeclaredMethod("onErrorFilter")
		};

		javaTermOps = new Method[]{
				java.util.stream.Stream.class.getDeclaredMethod("count"),
				java.util.stream.Stream.class.getDeclaredMethod("findFirst"),
				java.util.stream.Stream.class.getDeclaredMethod("reduce", BinaryOperator.class),
		};

		//the arguments used for the terminal operations (reduce())
		termArgs = new Object[][]{
				new Object[0],
				new Object[0],
				new Object[]{
						(BinaryOperator<Integer>) Integer::sum,
						(BinaryOperator<Integer>) (i1, i2) -> i1 + (i2 / 2) + (i1 - i2) * (i1 + i2),
						(BinaryOperator<Integer>) (i1, i2) -> i1 * i2,
						(BinaryOperator<Integer>) (i1, i2) -> i1 - i2,
						(BinaryOperator<Integer>) Math::max,
						(BinaryOperator<Integer>) Math::min
				}
		};

		rdm = new Random();
	}

	@Test
	void artemisExample1() {
		long expected = 0;
		long actual = Stream.of().count();

		assertEquals(expected, actual);
	}

	@Test
	void artemisExample2() {
		assertEquals(new ArrayList<>(),
					 Stream.of().toCollection(ArrayList::new));
	}

	@Test
	void artemisExample3() {
		assertEquals(3,
					 Stream.of(1, 2, 3).count());
	}

	@Test
	void artemisExample4() {
		ArrayList<Integer> expected = new ArrayList<>(List.of(1, 2, 3));

		assertEquals(expected,
					 Stream.of(1, 2, 3).toCollection(ArrayList::new));
	}

	@Test
	void artemisExample5() {
		assertEquals(1,
					 Stream.of(1, 2, 3).filter(i -> i % 2 == 0).count());
	}

	@Test
	void artemisExample6() {
		assertEquals(Optional.of(4),
					 Stream.of(1, 2, 3).map(i -> i * i).filter(i -> i % 2 == 0).findFirst());
	}

	@Test
	void artemisExample7() {
		long actual = Stream.of(1, 2, 3)
				.map(i -> {
					if (i % 2 == 0) {
						throw new IllegalArgumentException();
					}

					return i;
				}).count();

		assertEquals(3, actual);
	}

	@Test
	void artemisExample8() {
		ArrayList<Integer> actual = (ArrayList<Integer>) Stream.of(1, 2, 3, 4, 5)
				.map(i -> {
					if (i % 2 == 0)
						throw new IllegalArgumentException();
					return i;
				}).filter(i -> i > 1).onErrorMap(list -> 42).toCollection(ArrayList::new);

		ArrayList<Integer> expected = new ArrayList<>(List.of(42, 3, 42, 5));

		assertEquals(expected, actual);
	}

	@Test
	void artemisExample9() {
		ArrayList<String> actual = (ArrayList<String>) Stream.of(1, 2, 3, 4, 5, 6)
				.mapChecked(i -> {
					if (i % 3 == 0)
						throw new IOException("x:" + i);
					return i;
				}).filter(i -> i != 5)
				.map(i -> i == 4 ? null : i)
				.map(Object::toString)
				.onErrorMap(list -> list.get(0).getMessage())
				.toCollection(ArrayList::new);

		ArrayList<String> expected = new ArrayList<>();
		expected.add("1");
		expected.add("2");
		expected.add("x:3");
		expected.add(null);
		expected.add("x:6");

		assertEquals(expected, actual);
	}

	@Test
	void artemisExample10() {
		ArrayList<Integer> actual = (ArrayList<Integer>) Stream.of(1, 2, 3, 4, 5, 6)
				.map(i -> i / (i - 1))
				.distinct()
				.onErrorFilter()
				.toCollection(ArrayList::new);

		ArrayList<Integer> expected = new ArrayList<>(List.of(2, 1));

		assertEquals(expected, actual);
	}

	@Test
	void artemisExample11() {
		assertThrows(CheckedStreamException.class,
					 () -> Stream.of(1, 2, 3, 4, 5, 6).mapChecked(i -> {
						 if (i > 10)
							 throw new IOException("x:" + i);
						 return i;
					 }).reduce(Integer::sum));
	}

	@Test
	@DisplayName("Read files as checked Stream")
	void readFilesCheckedStream() {
		Stream<Path> stream = Stream.of(
				Paths.get("data", "1.txt"),
				Paths.get("data", "2.txt"),
				Paths.get("data", "nonexistent.txt")
		);

		assertEquals(2, stream.mapChecked(Files::readAllLines).map(list -> {
			StringBuilder sb = new StringBuilder();
			list.forEach(sb::append);
			return sb.toString();
		}).onErrorFilter().count());
	}

	@Test
	@DisplayName("Various 1")
	void various1() {
		Collection<Integer> even = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
				.mapChecked(i -> {
					if (i % 2 == 0) throw new Exception("Number even");
					return i;
				})
				.onErrorFilter()
				.toCollection(ArrayList::new);

		assertEquals(5, even.size());

		Collection<String> parity = Stream.of(even)
				.map(i -> (i > 3) ? i + 1 : i)
				.map(i -> i % 2 == 0)
				.map(b -> b ? "even" : "odd")
				.distinct()
				.toCollection(ArrayList::new);

		assertEquals(2, parity.size());

		Collection<String> filtered = Stream.of(parity)
				.filterChecked(str -> {
					if (str.length() == 3) throw new Exception("String length");
					return true;
				})
				.onErrorMapChecked(list -> "this errored")
				.onErrorFilter()
				.toCollection(ArrayList::new);

		assertEquals(List.of("this errored", "even"), filtered);
	}

	@Test
	@DisplayName("Set distinct property handling")
	void setIsDistinct() {
		Set<Integer> set = Set.of(1, 2, 3, 4);
		assertEquals(4, Stream.of(set).distinct().count());
		assertEquals(4, Stream.of(set).map(i -> 4).count());
		assertEquals(1, Stream.of(set).map(i -> 4).distinct().count());
	}

	@Test
	@DisplayName("Stream generation of null objects")
	@SuppressWarnings("ConstantConditions")
	void handlesNullInGeneration() {
		assertThrows(NullPointerException.class, () -> Stream.of((java.util.stream.Stream<Object>) null));
		assertThrows(NullPointerException.class, () -> Stream.of((Collection<Object>) null));
		assertThrows(NullPointerException.class, () -> Stream.of((Set<Object>) null));
		assertThrows(NullPointerException.class, () -> Stream.of((Object[]) null));
	}

	@Test
	@DisplayName("Stream from Stream")
	void generatesStreamFromStream() {
		assertEquals("[1, 2, 3, 4, 5]",
					 Stream.of(java.util.stream.Stream.of(1, 2, 3, 4, 5)).toCollection(ArrayList::new).toString());
	}

	@Test
	@DisplayName("Handles null elements")
	void handlesNull() {
		List<String> list = new ArrayList<>();
		list.add("Hello");
		list.add("Hello");
		list.add(null);
		list.add("World");
		list.add("!");

		assertEquals(5,
					 Stream.of(list).count());
		assertEquals(4,
					 Stream.of(list).distinct().count());
		assertEquals(4,
					 Stream.of(list).map(str -> str).distinct().count());
		assertEquals(3,
					 Stream.of(list).filter(Objects::nonNull).distinct().count());
		assertEquals(1,
					 Stream.of(list).filter(Objects::isNull).distinct().count());

		//intermediates handle null
		assertThrows(NullPointerException.class, () -> Stream.of(list).map(null));
		assertThrows(NullPointerException.class, () -> Stream.of(list).mapChecked(null));
		assertThrows(NullPointerException.class, () -> Stream.of(list).filter(null));
		assertThrows(NullPointerException.class, () -> Stream.of(list).filterChecked(null));

		List<Integer> singletonNull = (List<Integer>)
				Stream.of(rdm.ints(1_000L).boxed())
						.map(i -> (i % 2 == 0) ? i : null)
						.filter(Objects::isNull)
						.distinct()
						.toCollection(ArrayList::new);

		assertEquals(1, singletonNull.size());
		assertNull(singletonNull.get(0));
	}

	@Test
	@DisplayName("Handles null in firstElement correctly")
	void firstElementTerminalNull() {
		assertThrows(NullPointerException.class, () -> Stream.of((Object) null).findFirst());
		assertThrows(ErrorsAtTerminalOperationException.class, () -> Stream.of(1).map(i -> {
			throw new RuntimeException();
		}).findFirst());
	}

	@Test
	@DisplayName("Empty stream in operations")
	void emptyStream() {
		assertEquals(0, Stream.of().count());
		assertEquals(0, Stream.of(1, 2, 3, 4).filter(i -> i < 0).count());
		assertEquals(Optional.empty(), Stream.of(1, 2, 3, 4).filter(i -> i < 0).findFirst());
	}

	@Test
	@DisplayName("Provokes exceptions in checked stream")
	void streamExceptions() {
		//case 1: terminal operation on checked stream
		assertThrows(CheckedStreamException.class, () -> Stream.of(1, 2, 3, 4, 5).mapChecked(i -> {
			if (i == 1) throw new Exception();
			return i;
		}).count());

		assertEquals(4, Stream.of(1, 2, 3, 4, 5).map(i -> {
			if (i == 1) throw new RuntimeException();

			return i;
		}).onErrorFilter().count());

		//case 2: one element has a non-checked exception, is then collected
		//we shouldn't a CheckedStreamException, as it's not checked, but we should get an ErrorsAtTerminalOperationException
		assertThrows(ErrorsAtTerminalOperationException.class, () -> Stream.of(1, 2, 3, 4, 5).map(i -> {
			if (i == 1) throw new RuntimeException();

			return i;
		}).toCollection(ArrayList::new));

		//when it's filtered out
		assertEquals(2, Stream.of(1, 2, 3).filter(i -> {
			if (i == 1) throw new RuntimeException();
			return true;
		}).onErrorFilter().count());
	}

	@Test
	@DisplayName("No terminal operation")
	void noTerminal() {
		assertDoesNotThrow(() -> Stream.of(1, 2, 3, 4).filter(i -> true));
	}

	@Test
	@DisplayName("Operations spread out")
	void spreadOutOverObjects() {
		Stream<Integer> stream1 = Stream.of(1, 2, 3).map(i -> i + 1);
		Stream<Integer> stream2 = stream1.filter(i -> (i % 2) != 0);
		assertEquals(1, stream2.count());
	}

	@SuppressWarnings("unchecked")
	@ParameterizedTest
	@MethodSource("dataSetProvider")
	@DisplayName("Performs stream operations correctly")
	void reflectCompareJavaStream(Collection<Integer> dataSet) throws InvocationTargetException, IllegalAccessException {
		Stream<Integer> ours = Stream.of(dataSet);
		java.util.stream.Stream<Integer> javas = dataSet.stream();

		//run intermediates
		for (int i = 0; i < NUMBER_INTER_OPS; i++) {
			int index = rdm.nextInt(ourInterOps.length);
			int argIndex = (interArgs[index].length != 0) ? rdm.nextInt(interArgs[index].length) : -1;

			Method ourChosenIntermediate = ourInterOps[index];
			Method javaChosenIntermediate = javaInterOps[index];
			Object chosenArgument = (argIndex != -1) ? interArgs[index][argIndex] : null;

			if (chosenArgument != null) {
				ours = (Stream<Integer>) ourChosenIntermediate.invoke(ours, chosenArgument);
				javas = (java.util.stream.Stream<Integer>) javaChosenIntermediate.invoke(javas, chosenArgument);
			} else {
				ours = (Stream<Integer>) ourChosenIntermediate.invoke(ours);
				javas = (java.util.stream.Stream<Integer>) javaChosenIntermediate.invoke(javas);
			}
		}

		//run terminals
		int index = rdm.nextInt(ourTermOps.length);
		int argIndex = (termArgs[index].length != 0) ? rdm.nextInt(termArgs[index].length) : -1;

		Method ourChosenTerminal = ourTermOps[index];
		Method javaChosenTerminal = javaTermOps[index];
		Object chosenArgument = (argIndex != -1) ? termArgs[index][argIndex] : null;
		Object ourResult, javaResult;

		if (chosenArgument != null) {
			ourResult = ourChosenTerminal.invoke(ours, chosenArgument);
			javaResult = javaChosenTerminal.invoke(javas, chosenArgument);
		} else {
			ourResult = ourChosenTerminal.invoke(ours);
			javaResult = javaChosenTerminal.invoke(javas);
		}

		System.out.println("expected: " + javaResult.toString() + ", was: " + ourResult.toString());
		assertEquals(javaResult, ourResult);
	}

	static java.util.stream.Stream<Arguments> dataSetProvider() {
		return java.util.stream.Stream.of(
				Arguments.of(rdm.ints(RDM_STREAM_SIZE).boxed().collect(Collectors.toList())),
				Arguments.of(rdm.ints(RDM_STREAM_SIZE, Integer.MIN_VALUE, 0).boxed().collect(Collectors.toList())),
				Arguments.of(rdm.ints(RDM_STREAM_SIZE, 0, Integer.MAX_VALUE).boxed().collect(Collectors.toList())),
				Arguments.of(Collections.emptyList())
		);
	}
}