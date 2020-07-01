module.exports = function AbstractWorkerSnsCallTopicCommon() {
  var Promise;

  // 継承：親クラス＝AbstractBizCommon.js
  var superClazzFunc = require("./AbstractBizCommon.js");
  AbstractWorkerSnsCallTopicCommon.prototype = new superClazzFunc();

  // 各ログレベルの宣言
  var LOG_LEVEL_TRACE = 1;
  var LOG_LEVEL_DEBUG = 2;
  var LOG_LEVEL_INFO = 3;
  var LOG_LEVEL_WARN = 4;
  var LOG_LEVEL_ERROR = 5;

  // 現在の出力レベルを設定(ワーカー処理は別のログレベルを設定可能とする)
  var LOG_LEVEL_CURRENT = LOG_LEVEL_INFO;
  if (process && process.env && process.env.LogLevelForWorker) {
    LOG_LEVEL_CURRENT = process.env.LogLevelForWorker;
  }

  /*
  現在のログレベルを返却する
  ※　処理制御側とログレベルを同一設定で行う場合は、オーバーライドをコメントアウトする
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.getLogLevelCurrent = function () {
    return LOG_LEVEL_CURRENT;
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype);
  /*
  出力レベル毎のログ処理
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.writeLogTrace = function (
    msg
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelTrace() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype);

  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.writeLogDebug = function (
    msg
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelDebug() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype);

  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.writeLogInfo = function (
    msg
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelInfo() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype);

  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.writeLogWarn = function (
    msg
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelWarn() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype);

  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.writeLogError = function (
    msg
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelError() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype);

  // 処理の実行
  function* executeBizWorkerCommon(event, context, bizRequireObjects) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# executeBizWorkerCommon : start"
      );
      if (bizRequireObjects.PromiseObject) {
        Promise = bizRequireObjects.PromiseObject;
      }
      AbstractWorkerSnsCallTopicCommon.prototype.RequireObjects = bizRequireObjects;

      return yield AbstractWorkerSnsCallTopicCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# executeBizWorkerCommon : end"
      );
    }
  }
  AbstractWorkerSnsCallTopicCommon.prototype.executeBizWorkerCommon = executeBizWorkerCommon;

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# beforeMainExecute : start"
      );

      base.writeLogInfo(
        "AbstractWorkerSnsCallTopicCommon# beforeMainExecute:args:" +
          JSON.stringify(args)
      );

      return Promise.resolve(args);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# beforeMainExecute : end"
      );
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon);

  /*
  Streamの情報から新規登録の場合のみ通知するように判定する

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.extractBizInfos = function (
    args
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# extractBizInfos : start"
      );

      return new Promise(function (resolve, reject) {
        try {
          var record = base.getLastIndexObject(args);

          var skipFlag = "false";
          if (record && record.eventName && record.eventName == "INSERT") {
            resolve(record);
          } else {
            skipFlag = "true";
            base.writeLogTrace(
              "AbstractWorkerSnsCallTopicCommon# skipFlag:true"
            );
            var handlerParams = { skipFlag: skipFlag };
            resolve(handlerParams);
          }
        } catch (err) {
          reject(err);
        }
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# extractBizInfos : end"
      );
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon);

  /*
  SNSへの送信パラメータを生成する

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.transformRecordInfos = function (
    args
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# transformRecordInfos : start"
      );

      var record = base.getLastIndexObject(args);
      base.writeLogTrace(JSON.stringify(record));
      return new Promise(function (resolve, reject) {
        try {
          if (record && record.skipFlag && record.skipFlag == "true") {
            // 前処理でSkipフラグが立っているので処理なし
            resolve(record);
          } else {
            var topic = base.getTargetSnsTopicArn();
            var payload = {
              default: JSON.stringify(record.dynamodb),
            };

            var params = {
              TargetArn: topic,
              MessageStructure: "json",
              Message: JSON.stringify(payload),
            };

            resolve(params);
          }
        } catch (err) {
          reject(err);
        }
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# transformRecordInfos : end"
      );
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon);

  /*
  SNSへpublish処理を実行する

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# businessMainExecute : start"
      );

      var record = base.getLastIndexObject(args);

      return new Promise(function (resolve, reject) {
        try {
          if (record && record.skipFlag && record.skipFlag == "true") {
            // 前処理でSkipフラグが立っているので処理なし
            resolve(record);
          } else {
            base.RequireObjects.Sns.publish(record, function (error, data) {
              if (error) {
                base.printStackTrace(error);
                reject(error);
              } else {
                base.writeLogTrace(
                  "AbstractWorkerSnsCallTopicCommon# Sns Response:" +
                    JSON.stringify(data)
                );
                resolve(data);
              }
            });
          }
        } catch (err) {
          reject(err);
        }
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# businessMainExecute : end"
      );
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon);

  /*
  SNSへpublish処理を実行する

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.afterMainExecute = function (
    args
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# afterMainExecute : start"
      );

      var record = base.getLastIndexObject(args);

      var msg = "";
      var record = base.getLastIndexObject(args);
      if (record && record.skipFlag && record.skipFlag == "true") {
        msg = "Skip Record";
      } else {
        if (record && record.MessageId) {
          msg = msg + "SNS MessageId:" + record.MessageId + "#, ";
        }
        if (
          record &&
          record.ResponseMetadata &&
          record.ResponseMetadata.RequestId
        ) {
          msg = msg + "SNS RequestId:" + record.ResponseMetadata.RequestId + "";
        }
      }

      base.writeLogInfo("AbstractWorkerSnsCallTopicCommon# msg:" + msg);

      return new Promise(function (resolve, reject) {
        resolve("afterMainExecute Finish:" + msg);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerSnsCallTopicCommon# afterMainExecute : end"
      );
    }
  }.bind(AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon);

  /*
  直列処理のメソッド配列を返却する(オーバーライド)

  サンプル用にログ出力量を減らす為、後処理を間引く

  */
  AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon.getTasks = function (
    event,
    context
  ) {
    var base = AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractWorkerSnsCallTopicCommon# getTasks : start");
      return [
        this.beforeMainExecute,
        this.extractBizInfos,
        this.transformRecordInfos,
        this.businessMainExecute,
        this.afterMainExecute,
      ];
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractWorkerSnsCallTopicCommon# getTasks : end");
    }
  };

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    executeBizWorkerCommon,
    AbstractBaseCommon:
      AbstractWorkerSnsCallTopicCommon.prototype.AbstractBaseCommon,
    AbstractBizCommon: AbstractWorkerSnsCallTopicCommon.prototype,
  };
};
