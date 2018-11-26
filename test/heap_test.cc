# include <iostream>
# include <functional>
# include <vector>
# include <algorithm>

using namespace std;

void printVec(vector<int> nums)
{
    for (int i = 0; i < nums.size(); ++i)
        cout << nums[i] << " ";
    cout << endl;
}
int main(void)
{
    int nums_temp[] = {8, 3, 4, 8, 9, 2, 3, 4};
    vector<int> nums(nums_temp, nums_temp + 8);
    cout << "sorce nums: ";
    printVec(nums);

    cout << "(默认)make_heap: ";
    make_heap(nums.begin(), nums.end());
    printVec(nums);

    cout << "(less)make_heap: ";
    make_heap(nums.begin(), nums.end(), less<int> ());
    printVec(nums);

    cout << "(greater)make_heap: ";
    make_heap(nums.begin(), nums.end(), greater<int> ());
    printVec(nums);

    cout << "此时，nums为小顶堆" << endl;;
    cout << "push_back(3)" << endl;
    nums.push_back(3);
    cout << "忽略第三个参数，即为默认)push_heap: ";
    push_heap(nums.begin(), nums.end());
    printVec(nums);
    cout << "第三个参数为greater: ";
    push_heap(nums.begin(), nums.end(), greater<int>());
    printVec(nums);
    cout << "(做替换)pop_heap: ";
    pop_heap(nums.begin(), nums.end());
    printVec(nums);
    cout << "pop_back(): ";
    nums.pop_back();
    printVec(nums);
}
