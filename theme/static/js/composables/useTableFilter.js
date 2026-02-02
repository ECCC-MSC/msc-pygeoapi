import { ref, computed } from 'vue'

export default function useTableFilter(rows, keyColumns, defaultSortCol, tableTexti18n) {
  // sort and filtering
  const searchText = ref('')
  const searchTextLowered = computed(() => {
    return searchText.value.toLowerCase()
  })
  const currentSortDir = ref('asc')
  const currentSort = ref(defaultSortCol)
  const pageSize = ref(500)
  const currentPage = ref(1)
  const totalSize = computed(() => {
    return rows.value.length
  })

  // change sort direction
  const sortDir = function(colKey) {
    // if colKey == current sort, reverse
    if (colKey === currentSort.value) {
      currentSortDir.value = currentSortDir.value === 'asc' ? 'desc' : 'asc'
    }
    currentSort.value = colKey
  }

  // change sort icon
  const sortIconClass = computed(() => {
    if (currentSortDir.value === 'asc') {
      return 'glyphicon-sort-by-attributes'
    } else {
      return 'glyphicon-sort-by-attributes-alt'
    }
  })
  
  // sort function for rows
  const sortRows = function(a, b) {
    let modifier = 1
    if (currentSortDir.value === 'desc') {
      modifier = -1
    }
    if (a[currentSort.value] < b[currentSort.value]) {
      return -1 * modifier
    }
    if (a[currentSort.value] > b[currentSort.value]) {
      return 1 * modifier
    }
    return 0
  }

  // filter function for rows
  const searchFilter = function(row) {
    if (searchText.value !== '') {
      let rowText = ''

      // concatenate all properties to a single text for ease of search
      Object.keys(row).forEach((key) => {
        if (!keyColumns.value.includes(key)) {
          return false
        } else if (row[key] === null) {
          return false // skip null values
        } else {
          const valueText = row[key] + '' // ensure to string
          rowText += '^' + valueText.toLowerCase() + '$' // hidden starts with and ends search helper; like regex
        }
      })

      if (rowText.includes(searchTextLowered.value)) {
        return true
      } else {
        return false
      }
    } else {
      return true
    }
  }

  // pagination
  const paginateFilter = function (row, index) {
    const start = (currentPage.value - 1) * pageSize.value
    const end = currentPage.value * pageSize.value
    if (index >= start && index < end) {
      return true
    } else {
      return false
    }
  }
  const nextPage = function () {
    if ((currentPage.value * pageSize.value) < filteredNumEntries.value) {
      currentPage.value++
    }
  }
  const prevPage = function () {
    if (currentPage.value > 1) {
      currentPage.value--
    }
  }
  const filteredNumEntries = computed(() => {
    return filteredRows.value.length
  })
  const startEntryOfPage = computed(() => {
    if (filteredNumEntries.value === 0) {
      return 0
    } else if (currentPage.value === 1) {
      return currentPage.value
    } else {
      return ((currentPage.value - 1) * pageSize.value) + 1
    }
  })
  const lastEntryOfPage = computed(() => {
    const maxEntryThisPage = currentPage.value * pageSize.value
    if (maxEntryThisPage < filteredNumEntries.value) {
      return maxEntryThisPage
    } else {
      return filteredNumEntries.value
    }
  })
  // const maxPages = computed(() => {
  //   return Math.ceil(filteredNumEntries / pageSize)
  // })
  const filteredFromText = computed(() => {
    if (filteredNumEntries.value < totalSize.value) {
      if (totalSize.value === 1) {
        // singular case
        return tableTexti18n.filteredSingular.replace('$totalSize', totalSize.value)
      } else { // > 1
        // plural case
        return tableTexti18n.filteredPlural.replace('$totalSize', totalSize.value)
      }
    } else {
      return ''
    }
  })
  const showingFilteredFromText = computed(() => {
    if (filteredFromText.value !== '') {
      return filteredNumEntries.value + ' ' + filteredFromText.value
    } else {
      return ''
    }
  })
  const showingFilterText = computed(() => {
    let showText = tableTexti18n.showing
      .replace('$startEntryOfPage', startEntryOfPage.value)
      .replace('$lastEntryOfPage', lastEntryOfPage.value)
      .replace('$lastEntryOfPage', lastEntryOfPage.value)
    if (filteredFromText.value !== '') {
      showText += ` (${filteredFromText.value})`
    }
    return showText
  })

  const filteredRows = computed(() => {
    return rows.value
      .filter(searchFilter)
      .sort(sortRows)
  })
  const paginatedRows = computed(() => {
    return filteredRows.value
      .filter(paginateFilter)
  })

  // html/string modications for table presentation
  const stripTags = function(htmlString) {
    return htmlString.replace(/<[^>]+>/g, '')
  }
  const truncate = function (str, num) {
    if (str.length <= num) {
      return str
    }
    return str.slice(0, num) + '...'
  }
  const truncateStripTags = function(str) {
    return truncate(stripTags(str), 350)
  }
  const linkToRow = function(row, key, itemPath, locale) {
    if (key === 'id') {
      return `<a href="${itemPath + '/' + row[key]}?lang=${locale}">${row[key]}</a>`
    } else if (key === 'links') {
      let linksList = ''
      row[key].forEach(element =>
        linksList += '<li><a href="' + element['href'] + '">' + element['title'] + '</a></li>'
      ) 
      return `${linksList}`
    } else if (typeof row[key] === 'object') {
      return JSON.stringify(row[key], null, 2)
    } else {
      if (typeof(row[key]) === 'string') {
        return row[key].replace(/\n/g, '<br/>')
      }
      return row[key]
    }
  }

  return {
    filteredRows, searchText, searchTextLowered,
    currentSortDir, currentSort, sortDir, sortIconClass, 
    pageSize, currentPage, paginatedRows, prevPage, nextPage, showingFilterText, showingFilteredFromText,
    truncateStripTags, stripTags, truncate, linkToRow
  }
}
