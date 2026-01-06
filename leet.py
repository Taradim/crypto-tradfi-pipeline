import heapq
from collections import Counter
from typing import List
nums = [3,5,4,7,8,9]

class Solution:
    def kSmallestPairs(self, nums1: List[int], nums2: List[int], k: int) -> List[List[int]]:
        heap=[]

        for j in nums2:
            for i in nums1:
                if len(heap) < k:
                    heapq.heappush(heap, (-i-j,i,j))
                else:
                    if -i-j > heap[0][0] and j<heap[0][2]:
                        heapq.heappop(heap)
                        heapq.heappush(heap, (-i-j,i,j))
        return [[n,m] for _, n,m in heap]
                