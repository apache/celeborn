package org.apache.spark.shuffle.celeborn

import net.bytebuddy.asm.Advice
import net.bytebuddy.ByteBuddy
import net.bytebuddy.agent.ByteBuddyAgent
import net.bytebuddy.description.modifier.Visibility
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy
import net.bytebuddy.implementation.{FieldAccessor, MethodCall, MethodDelegation}
import net.bytebuddy.matcher.ElementMatchers.named
import net.bytebuddy.dynamic.ClassFileLocator
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.matcher.ElementMatchers
import net.bytebuddy.pool.TypePool
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{MASTER_ENDPOINTS, SPARK_SHUFFLE_WRITER_MODE}
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.tests.spark.SparkTestBase
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * @time 2023/11/28 11:03 PM
 * @author fchen <cloud.chenfu@gmail.com>
 */
class ColumnarShuffleSuite extends AnyFunSuite with MiniClusterFeature {
  test("aa") {

    //    new ByteBuddy()
    //      .redefine(classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec])
    //    .defineField("_id", int.class, Visibility.PUBLIC)
    //    .make()
    //      .load(MyDocument.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
    //
    //    classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec].getDeclaredFields
    //      .foreach(f => println(f.getName))
    //    println("1====")
    //    classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec]
    //    .getDeclaredConstructors
    //      .foreach { c =>
    //        println("=====")
    //        c.getParameterTypes.foreach(t => println(t.getName))
    //      }

    ByteBuddyAgent
      .install()
    //     new AgentBuilder.Default()
    //            .disableClassFormatChanges()
    //            .with(AgentBuilder.RedefinitionStrategy.REDEFINITION)
    //            .type(ElementMatchers.named("pkg.MapText"))
    //            .transform(new AgentBuilder.Transformer() {
    //                @Override
    //                public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder,
    //                TypeDescription typeDescription, ClassLoader classLoader, JavaModule
    //                javaModule) {
    //                    return builder.defineMethod("save", void.class, Visibility.PUBLIC)
    //                            .intercept(MethodDelegation.to(ActiveRecordInterceptor.class));
    //                }
    //            }).with(new ListenerImpl()).installOnByteBuddyAgent();
    //    CallTextSave callTextSave =  new CallTextSave();
    //    callTextSave.save();


    val typePool = TypePool.Default.ofSystemLoader()
    val classLoader = Utils.getContextOrSparkClassLoader
    val classReloadingStrategy = ClassReloadingStrategy.fromInstalledAgent()

    // 方案1
    val clz = new ByteBuddy()
      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do not use
        // 'Bar.class'
        ClassFileLocator.ForClassLoader.ofSystemLoader())
      //      .redefine(classOf[org.apache.spark.ShuffleDependency[_, _, _]])
      //      .field(named"xyz")
      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility.PUBLIC)
      .make()
      //      .load(classLoader, classReloadingStrategy)
      .load(Utils.getSparkClassLoader, ClassLoadingStrategy.Default.INJECTION)
      .getLoaded


    // 方案2
    val types1: Seq[Class[_]] = Seq(
      classOf[org.apache.spark.rdd.RDD[_]],
      classOf[org.apache.spark.Partitioner],
      classOf[org.apache.spark.serializer.Serializer],
      classOf[scala.Option[_]],
      classOf[scala.Option[_]],
      classOf[Boolean],
      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]])
    val types2: Seq[Class[_]] = Seq(
      classOf[org.apache.spark.rdd.RDD[_]],
      classOf[org.apache.spark.Partitioner],
      classOf[org.apache.spark.serializer.Serializer],
      classOf[scala.Option[_]],
      classOf[scala.Option[_]],
      classOf[Boolean],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]])
    val parameterTypes: java.util.List[Class[_]] = java.util.Arrays.asList[Class[_]](types2: _*)
    val columnarClz = new ByteBuddy()

      .subclass(clz, ConstructorStrategy.Default.NO_CONSTRUCTORS)
      //      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do
      //      not use 'Bar.class'
      //        ClassFileLocator.ForClassLoader.ofSystemLoader())
      //      .redefine(Class.forName("org.apache.spark.ShuffleDependency"))
      //      .redefine(classOf[org.apache.spark.ShuffleDependency[_, _, _]])
      //      .redefine(clz)
      //      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility
      //      .PUBLIC)
      .name("org.apache.spark.CelebornColumnarShuffleDependency")
      .defineConstructor(Visibility.PUBLIC)
      .withParameters(parameterTypes)
      .intercept(
        MethodCall.invoke(
          //          Class.forName("org.apache.spark.ShuffleDependency").getConstructor
          //          (types1: _*))
          Class.forName("org.apache.spark.ShuffleDependency").getDeclaredConstructors.head)
          .withArgument(0, 1, 2, 3, 4, 5, 7, 8, 9, 10)
          .andThen(FieldAccessor.ofField("schema").setsArgumentAt(6))
      )
      .make()
      //      .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
      .load(Utils.getSparkClassLoader, classReloadingStrategy)
      .getLoaded


    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredFields
      .foreach(f => println(f.getName))
    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredConstructors
      .foreach { c =>
        println("=====")
        c.getParameterTypes.foreach(t => println(t.getName))
      }

    println("columnar")
    Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
      .getDeclaredConstructors
      .foreach { c =>
        println("=====")
        c.getParameterTypes.foreach(t => println(t.getName))
      }


    val method = classOf[JavaShuffleExchangeExecInterceptor]
      .getDeclaredMethods
      .filter(m => m.getName == "intercept")
      .head
    //    new ByteBuddy()
    //      .redefine(ShuffleExchangeExec.getClass)
    ////      .redefine(Class.forName("org.apache.spark.sql.execution.exchange
    // .ShuffleExchangeExec$"))
    //      .method(ElementMatchers.named("prepareShuffleDependency"))
    //      .intercept(MethodDelegation.to(new JavaShuffleExchangeExecInterceptor))
    ////      .intercept(MethodCall.invoke(method))
    //      .make()
    //      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)

    // ----- -----
//    new ByteBuddy()
////      .redefine(org.apache.spark.sql.ShuffleExchangeExec.getClass)
////      .redefine(org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.getClass)
////      .redefine(Class.forName("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$"))
//      .redefine(typePool.describe("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec").resolve(), // do not use
//        // 'Bar.class'
//        ClassFileLocator.ForClassLoader.ofSystemLoader())
//      .visit(Advice.to(classOf[JavaShuffleExchangeExecInterceptorV2]).on(named("prepareShuffleDependency")))
//      .make()
//      .load(Utils.getSparkClassLoader, classReloadingStrategy)


    new ByteBuddy()
      //      .redefine(ShuffleExchangeExec.getClass)
      //      .redefine(Class.forName("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$"))

      .rebase(typePool.describe("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$").resolve(), // do not use
        // 'Bar.class'
        ClassFileLocator.ForClassLoader.ofSystemLoader())
      .method(ElementMatchers.named("prepareShuffleDependency"))
      .intercept(MethodDelegation.to(classOf[JavaShuffleExchangeExecInterceptor]))
      //      .intercept(MethodCall.invoke(method))
      .make()
      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)

    setUpMiniCluster(workerNum = 1)


    val spark = SparkSession.builder()
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      //      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .config(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .config("spark.shuffle.service.enabled", "false")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .config(s"spark.${CelebornConf.COLUMNAR_SHUFFLE_ENABLED.key}", "true")
      .config(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .config(s"spark.${CelebornConf.SPARK_SHUFFLE_WRITER_MODE.key}", "hash")
      .master("local[2]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    val cnt = spark.range(0, 1000, 1, 2)
      .select(Seq(col("id"), hash(col("id")).cast("string").as("rid")): _*)
      .repartition(10, Seq(col("id"), col("rid")): _*)
      .count()
    println(cnt)

  }

  test("bytebuddy") {
    ByteBuddyAgent
      .install()
    val typePool = TypePool.Default.ofSystemLoader()
    val classLoader = Utils.getContextOrSparkClassLoader
    val classReloadingStrategy = ClassReloadingStrategy.fromInstalledAgent()

    // 方案1
    val clz = new ByteBuddy()
      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do not use
        // 'Bar.class'
        ClassFileLocator.ForClassLoader.ofSystemLoader())
      //      .redefine(classOf[org.apache.spark.ShuffleDependency[_, _, _]])
      //      .field(named"xyz")
      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility.PUBLIC)
      .make()
      //      .load(classLoader, classReloadingStrategy)
      .load(Utils.getSparkClassLoader, ClassLoadingStrategy.Default.INJECTION)
      .getLoaded


    // 方案2
    val types1: Seq[Class[_]] = Seq(
      classOf[org.apache.spark.rdd.RDD[_]],
      classOf[org.apache.spark.Partitioner],
      classOf[org.apache.spark.serializer.Serializer],
      classOf[scala.Option[_]],
      classOf[scala.Option[_]],
      classOf[Boolean],
      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]])
    val types2: Seq[Class[_]] = Seq(
      classOf[org.apache.spark.rdd.RDD[_]],
      classOf[org.apache.spark.Partitioner],
      classOf[org.apache.spark.serializer.Serializer],
      classOf[scala.Option[_]],
      classOf[scala.Option[_]],
      classOf[Boolean],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]])
    val parameterTypes: java.util.List[Class[_]] = java.util.Arrays.asList[Class[_]](types2: _*)
    val columnarClz = new ByteBuddy()

      .subclass(clz, ConstructorStrategy.Default.NO_CONSTRUCTORS)
      //      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do
      //      not use 'Bar.class'
      //        ClassFileLocator.ForClassLoader.ofSystemLoader())
      //      .redefine(Class.forName("org.apache.spark.ShuffleDependency"))
      //      .redefine(classOf[org.apache.spark.ShuffleDependency[_, _, _]])
      //      .redefine(clz)
      //      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility
      //      .PUBLIC)
      .name("org.apache.spark.CelebornColumnarShuffleDependency")
      .defineConstructor(Visibility.PUBLIC)
      .withParameters(parameterTypes)
      .intercept(
        MethodCall.invoke(
          //          Class.forName("org.apache.spark.ShuffleDependency").getConstructor
          //          (types1: _*))
          Class.forName("org.apache.spark.ShuffleDependency").getDeclaredConstructors.head)
          .withArgument(0, 1, 2, 3, 4, 5, 7, 8, 9, 10)
          .andThen(FieldAccessor.ofField("schema").setsArgumentAt(6))
      )
      .make()
      //      .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
      .load(Utils.getSparkClassLoader, classReloadingStrategy)
      .getLoaded


    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredFields
      .foreach(f => println(f.getName))
    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredConstructors
      .foreach { c =>
        println("=====")
        c.getParameterTypes.foreach(t => println(t.getName))
      }

    println("columnar")
    Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
      .getDeclaredConstructors
      .foreach { c =>
        println("=====")
        c.getParameterTypes.foreach(t => println(t.getName))
      }


//    val method = classOf[JavaShuffleExchangeExecInterceptor]
//      .getDeclaredMethods
//      .filter(m => m.getName == "intercept")
//      .head
    new ByteBuddy()
//      .redefine(ShuffleExchangeExec.getClass)
//      .redefine(Class.forName("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$"))

      .rebase(typePool.describe("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$").resolve(), // do not use
        // 'Bar.class'
        ClassFileLocator.ForClassLoader.ofSystemLoader())
      .method(ElementMatchers.named("prepareShuffleDependency"))
      .intercept(MethodDelegation.to(classOf[JavaShuffleExchangeExecInterceptor]))
      //      .intercept(MethodCall.invoke(method))
      .make()
      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)

//    new ByteBuddy()
//      .redefine(ShuffleExchangeExec.getClass)
//      .visit(Advice.to(classOf[JavaShuffleExchangeExecInterceptorV2]).on(named("prepareShuffleDependency")))
//      .make()
//      .load(Utils.getSparkClassLoader, classReloadingStrategy)

  }

  test("cc") {

//    classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec]
//      .getDeclaredFields
//      .foreach(f => println(f.getName))
//    org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.getClass
//      .getDeclaredFields
//      .foreach(f => println(f.getName))
      Class.forName("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$")
        .getDeclaredFields
        .foreach(f => println(f.getType))
  }
}

