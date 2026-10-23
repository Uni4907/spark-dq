// Always create independent form rows, including nested mappings and sort order.
export function policyForm(policy){
 return {dropMissing:policy.dropMissing===true,deduplicate:policy.deduplicate===true,
  fills:Object.entries(policy.fills||{}).map(([column,value])=>({column,value})),
  normalizations:Object.entries(policy.normalizations||{}).map(([column,operation])=>({column,operation})),
  mappings:Object.entries(policy.mappings||{}).map(([column,pairs])=>({column,pairs:pairs.map(p=>({from:p.from,to:p.to}))})),
  order:(policy.deduplicationOrder||[]).map(row=>({...row}))};
}
