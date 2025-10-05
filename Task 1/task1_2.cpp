#include <iostream>
#include <vector>
#include <tuple>
#include <limits>
#include <unordered_map>
#include <string>
#include <iomanip>
#include <algorithm>

struct Table {
    double rows;  
    std::unordered_map<std::string, double> cardinalities;  
};

struct JoinResult {
    double cost;  
    std::string plan;  
    double resultRows;  
};

std::vector<Table> tables;
std::vector<std::tuple<int, int, std::string, std::string>> joinPredicates;  
std::vector<std::tuple<int, std::string>> scanPredicates;  

int l; 

double applyScanPredicate(int tableIndex, const std::string& filterAttr) {
    double cardinality = tables[tableIndex - 1].cardinalities[filterAttr];
    double rows = tables[tableIndex - 1].rows;

    return rows / cardinality; 
}

double getCardinalityForJoin(
    int left, 
    int right, 
    const std::string& joinAttr1, 
    const std::string& joinAttr2
) {
    double rowsTimes = tables[left - 1].rows * tables[right - 1].rows;  

    double maxOfAttr = std::max(tables[left - 1].cardinalities[joinAttr1], tables[right - 1].cardinalities[joinAttr2]);  

    if (maxOfAttr == 0)
        return std::numeric_limits<double>::max(); 

    return rowsTimes / maxOfAttr;  
}

JoinResult computeJoinCost(
    int left, 
    int right, 
    const std::string& joinAttr1, 
    const std::string& joinAttr2,
    const JoinResult&  leftResult,
    const JoinResult&  rightResult
) {
    double leftRows = tables[left - 1].rows;  
    double rightRows = tables[right - 1].rows;  
    double cardinality = getCardinalityForJoin(left, right, joinAttr1, joinAttr2);  

    if (cardinality == std::numeric_limits<double>::max())
        return { std::numeric_limits<double>::max(), "", 0 };

    double resultRows = (leftRows * rightRows) / cardinality;  

    double hashJoinCost   = leftResult.cost + (leftRows * 3.5) 
    + rightResult.cost + (rightRows * 1.5) + (resultRows * 0.1);

    double nestedLoopCost = leftRows * rightRows + (resultRows * 0.1);

    double cost = std::min(hashJoinCost, nestedLoopCost);  

    std::string plan = "(" + std::to_string(left) + " " 
    + std::to_string(right) + " {" + joinAttr1 + " " 
    + joinAttr2 + "})"; 
    
    return { cost, plan, resultRows };  
}

JoinResult computeJoinTreeCost(const std::vector<int>& tablesToJoin) {
    if (tablesToJoin.size() == 1) {  
        int tableIndex = tablesToJoin[0];
        std::string plan = std::to_string(tableIndex);
        double resultRows = tables[tableIndex - 1].rows;
        for (const auto& filter : scanPredicates) { 
            int filterTableNum;
            std::string filterAttr;
            std::tie(filterTableNum, filterAttr) = filter;

            // if current attribute is member of current table
            if (filterTableNum == tableIndex) {
                plan += filterAttr;
                resultRows = applyScanPredicate(tableIndex, filterAttr); 
            }
        }

        return { 0, plan, resultRows }; 
    }

    double minCost = std::numeric_limits<double>::max();
    std::string bestPlan;
    double bestResultRows = 0;

    for (size_t i = 1; i < tablesToJoin.size(); ++i) {
        std::vector<int> leftTables(
            tablesToJoin.begin(), 
            tablesToJoin.begin() + i
        );

        std::vector<int> rightTables(
            tablesToJoin.begin() + i, 
            tablesToJoin.end()
        );

        JoinResult leftResult = computeJoinTreeCost(leftTables);  
        JoinResult rightResult = computeJoinTreeCost(rightTables);  
        bool foundJoin = false;  

        // seach join for current tables
        for (const auto& joinPred : joinPredicates) {
            auto [
                table1, 
                table2, 
                attr1, 
                attr2
            ] = joinPred;

            // check predicate left or right 
            if ((std::find(leftTables.begin(), leftTables.end(), table1) != leftTables.end() &&
                 std::find(rightTables.begin(), rightTables.end(), table2) != rightTables.end()) ||
                (std::find(leftTables.begin(), leftTables.end(), table2) != leftTables.end() &&
                 std::find(rightTables.begin(), rightTables.end(), table1) != rightTables.end())) {
                    
                JoinResult joinResult = computeJoinCost(table1, table2, attr1, attr2, leftResult, rightResult);  
                double currentCost = leftResult.cost + rightResult.cost + joinResult.cost;  

                if (currentCost < minCost) { 
                    minCost = currentCost;
                    bestPlan = "(" + leftResult.plan + " " + rightResult.plan +
                               " {" + std::to_string(table1) + "." + attr1 +
                               " " + std::to_string(table2) + "." + attr2 + "})";  

                    bestResultRows = joinResult.resultRows;  

                    foundJoin = true;
                }
            }
        }

        if (!foundJoin) {
            double currentCost = leftResult.cost + rightResult.cost + (leftResult.resultRows * rightResult.resultRows);

            if (currentCost < minCost) {       
                minCost = currentCost;
                bestPlan = "(" + leftResult.plan + " " + rightResult.plan + ")";  
                bestResultRows = leftResult.resultRows * rightResult.resultRows;  
            }
        }
    }

    return { minCost, bestPlan, bestResultRows };  
}

void readInput() {
    std::cin >> l;  
    tables.resize(l);  

    for (int i = 0; i < l; ++i)
        std::cin >> tables[i].rows; 

    int filterCount;  
    std::cin >> filterCount;

    for (int i = 0; i < filterCount; ++i) {
        int tableNum;
        std::string filterAttr;
        double cardValue;
        std::cin >> tableNum >> filterAttr >> cardValue;  
        tables[tableNum - 1].cardinalities[filterAttr] = cardValue;  
    }

    int scanPredCount;  

    std::cin >> scanPredCount;

    for (int i = 0; i < scanPredCount; ++i) {
        int tableNum;
        std::string filterAttr;
        std::cin >> tableNum >> filterAttr;
        scanPredicates.emplace_back(tableNum, filterAttr);  
    }

    int joinPredCount;  
    std::cin >> joinPredCount;

    for (int i = 0; i < joinPredCount; ++i) {  
        int table1, table2;
        std::string attr1, attr2;
        std::cin >> table1 >> table2 >> attr1 >> attr2; 
        joinPredicates.emplace_back(table1, table2, attr1, attr2);  
    }
}

int main() {
    readInput();  
    std::vector<int> tablesToJoin;

    for (int i = 1; i <= l; ++i)
        tablesToJoin.push_back(i);  

    JoinResult result = computeJoinTreeCost(tablesToJoin);  
    std::cout << result.plan << " "
              << std::fixed  << std::setprecision(2) 
              << result.cost << std::endl;
}
