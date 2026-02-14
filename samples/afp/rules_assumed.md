# Rules Used For Samples

For each input row:

1. `po_remaining = PO_value - Total_Claimed`
2. `category_remaining = Category_Limit - Total_Claimed`
3. If `po_remaining <= 0` OR `category_remaining <= 0`:
   - `certification = deauthorized`
   - `certified_cost = 0`
   - Do not update totals.
4. Else if `cost_amount <= min(po_remaining, category_remaining)`:
   - `certification = authorized`
   - `certified_cost = cost_amount`
   - Add `certified_cost` to both PO and category `Total_Claimed`.
5. Else:
   - `certification = partially_authorized`
   - `certified_cost = min(po_remaining, category_remaining)`
   - Add `certified_cost` to both PO and category `Total_Claimed`.
