package org.oursight.demo.spark.util;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class MyTokenizer {

  public static List<String> apply(String content) {
    List<Term> termList = NotionalTokenizer.segment((content));
    List<String> keywordList = new ArrayList<>();
    for (Term term :
        termList) {
      if(StringUtils.isNotBlank(term.word)) {
        keywordList.add(term.word);
      }
    }
    return keywordList;
  }

  public static String apply(String content, String separator){
    List<String> strs = apply(content);
    StringBuilder outText = new StringBuilder();
    for (String str : strs) {
      String trimStr = str.trim();
      if(StringUtils.isNotBlank(trimStr)) {
        outText.append(trimStr).append(separator);
      }
    }
    return outText.toString();
  }

  public static void main(String[] args) {
    System.out.println(MyTokenizer.apply("深圳泊众国内最专业棋牌游戏开发商 国内最大棋牌游戏开发商 首    页 精品手游 街机电玩 休闲棋牌 1分钟快速了解 选产品 产品选对基本就成功了一半完善棋牌产品包含游戏大厅网站前后台代理及运营系统等 点击在线咨询更多 问售后 良好的服务是持久成功的保障泊众提供免费培训架设Bug修正1对1专职售后等服务 点击在线咨询更多 看公司 选择优秀的棋牌游戏开发公司对运营商来说至关重要评价标准包括公司资质团队经验公司规模 购买产品上线运营 确认需求选择直接购买游戏和平台或定制开发游戏打造属于自己的棋牌游戏平台上线运营 点击在线咨询更多 购买现在游戏最快3天上线 多款热门游戏快速吸引玩家 李逵劈鱼 金鲨银鲨 斗地主 智勇三张 百变牛牛 二八杠 三十秒 至尊五张 奔驰宝马 二人雀神 血战麻将 跑胡子 更多游戏点击在线咨询 强大平台助推棋牌运营 十年沉淀之作棋牌行业最专业功能丰富强大经过持续运营验证 专业高端类型紫金游 经典界面布局玩家易接受上手快平台及游戏开源支持二次开发 大众休闲类型面对面 耗时一年倾力打造全新游戏平台平台采用DX技术开发 防作弊竞技类型同桌游 黑灰极简风格突显棋牌竞技本质多重防作弊技术游戏公平公正 黑色简约竞技风云版 完美售后服务为运营成功添筹码 •5天免费棋牌系统运营培训 •1年免费技术支持服务 •78小时专职在线 •永久免费BUG修复 •系统安装一条龙服务 •包教会使用棋牌软件 电话咨询 官方热线点击拨打"," "));
//    try {
//      List<String> lines = FileUtils.readLines(new File("E:/doubleNeg.txt"));
//      for(String line : lines){
//        System.out.println(Tokenizer.apply(line," "));
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }

}
