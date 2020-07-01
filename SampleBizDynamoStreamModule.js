/*
処理固有に必要な処理などを、この層に実装する。

テストや挙動確認を含めたコードをコメントアウト込みで、
サンプルとして記述する。

written by syo
http://awsblog.physalisgp02.com
*/
module.exports = function SampleBizDynamoStreamModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = new require("./AbstractDynamoDbStreamCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleBizDynamoStreamModule.prototype = new superClazzFunc();

  // 処理の実行
  function* execute(event, context, bizRequireObjects) {
    var base = SampleBizDynamoStreamModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleBizDynamoStreamModule# execute : start");

      // 親の業務処理を実行
      return yield SampleBizDynamoStreamModule.prototype.executeBizUnitCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleBizDynamoStreamModule# execute : end");
    }
  }

  /*
  ワーカー処理（疑似スレッド処理）用のクラスを返却
  */
  SampleBizDynamoStreamModule.prototype.AbstractBaseCommon.getSubWorkerClazzFunc = function () {
    var base = SampleBizDynamoStreamModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleBizDynamoStreamModule# getSubWorkerClazzFunc : start"
      );
      return require("./SampleBizWorkerSnsCallTopicModule.js");
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "SampleBizDynamoStreamModule# getSubWorkerClazzFunc : end"
      );
    }
  };

  return {
    execute,
  };
};
