#ifndef SAKURA_COST_MODEL_H__
#define SAKURA_COST_MODEL_H__

namespace wing {

class CostCalculator{
 public:
  static double HashJoinCost(double build_size, double probe_size) {
    return 0;
  }
  static double NestloopJoinCost(double build_size, double probe_size) {
    return 0;
  }
  static double SeqScanCost(double size) {
    return 0;
  }
};

}

#endif