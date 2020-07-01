module.exports = function SampleBizWorkerSnsCallTopicModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractWorkerSnsCallTopicCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleBizWorkerSnsCallTopicModule.prototype = new superClazzFunc();

  // 呼び出し先のSNSトピックのARNを環境変数から読み込む
  var TARGET_SNS_TOPIC_ARN = "";
  if (process && process.env && process.env.TargetSnsTopicArn) {
    TARGET_SNS_TOPIC_ARN = process.env.TargetSnsTopicArn;
  }

  function* execute(event, context, RequireObjects) {
    var base = SampleBizWorkerSnsCallTopicModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleBizWorkerSnsCallTopicModule# execute : start");

      return yield SampleBizWorkerSnsCallTopicModule.prototype.executeBizWorkerCommon(
        event,
        context,
        RequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleBizWorkerSnsCallTopicModule# execute : end");
    }
  }
  SampleBizWorkerSnsCallTopicModule.prototype.execute = execute;

  SampleBizWorkerSnsCallTopicModule.prototype.AbstractBaseCommon.getTargetSnsTopicArn = function () {
    var base = SampleBizWorkerSnsCallTopicModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleBizWorkerSnsCallTopicModule# getTargetSnsTopicArn : start"
      );
      return TARGET_SNS_TOPIC_ARN;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "SampleBizWorkerSnsCallTopicModule# getTargetSnsTopicArn : end"
      );
    }
  }.bind(SampleBizWorkerSnsCallTopicModule);

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    execute,
  };
};
