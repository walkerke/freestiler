.cluster_fixture <- function(n=4L) {
  skip_if_not_installed("arrow")
  x <- sf::st_as_sf(data.frame(x=if (n>1000) seq(-170,170,length.out=n) else seq(-97,-96.9999,length.out=n),y=32,
    group_id=rep(1:2,length.out=n),label=paste0("row",seq_len(n))),coords=c("x","y"),crs=4326)
  path <- tempfile(fileext=".parquet")
  geom <- arrow::Array$create(lapply(sf::st_as_binary(sf::st_geometry(x)),unclass),type=arrow::binary())
  arrow::write_parquet(arrow::arrow_table(group_id=x$group_id,label=x$label,geometry=geom),path)
  path
}
.cluster_available <- function() {
  result <- .Call(wrap__rust_cluster_file,"","","","mvt",0L,0L,60.,2L,"group_id","[1,2]",TRUE)
  !grepl("requires the GeoParquet-enabled build",result,fixed=TRUE)
}

test_that("ordered categorical files use shared core, typed counts and provenance", {
  skip_on_cran();skip_if_not(.cluster_available())
  input <- .cluster_fixture();output <- tempfile(fileext=".pmtiles")
  on.exit(unlink(c(input,output)),add=TRUE)
  for (format in c("mvt","mlt")) {
    result <- freestile_file(input,output,min_zoom=0,max_zoom=3,cluster_maxzoom=3,
      cluster_distance=60,category="group_id",category_values=1:2,tile_format=format,quiet=TRUE)
    a <- attr(result,"cluster_audit")
    expect_equal(a$points,4);expect_equal(a$categories,c(2,2,0));expect_true(all(a$levels$people==4))
    m <- pmtiles_metadata(output)
    expect_equal(m$tile_format,format)
    expect_equal(m$metadata$freestiler$people,4)
    expect_equal(m$metadata$vector_layers[[1]]$fields$point_count,"Number")
    expect_false("point_count_abbreviated"%in%names(m$metadata$vector_layers[[1]]$fields))
  }
})

test_that("categorical file limitations fail explicitly instead of silently changing algorithms", {
  skip_on_cran();skip_if_not(.cluster_available())
  input <- .cluster_fixture();output <- tempfile(fileext=".pmtiles")
  on.exit(unlink(c(input,output)),add=TRUE)
  args <- list(input=input,output=output,min_zoom=0,max_zoom=3,cluster_maxzoom=3,
    cluster_distance=60,category="group_id",category_values=1:2,quiet=TRUE)
  expect_error(do.call(freestile_file,modifyList(args,list(base_zoom=3))),"base_zoom = NULL")
  expect_error(do.call(freestile_file,modifyList(args,list(simplification=FALSE))),"simplification = TRUE")
  expect_error(do.call(freestile_file,modifyList(args,list(cluster_maxzoom=2))),"separate dot source")
  expect_error(do.call(freestile_file,modifyList(args,list(cluster_distance=NaN))),"positive cluster_distance")
  expect_error(do.call(freestile_file,modifyList(args,list(category_values=c(1,1)))),"distinct")
  expect_error(do.call(freestile_file,modifyList(args,list(drop_rate=2))),"conserve")
  expect_error(do.call(freestile_file,modifyList(args,list(category="missing"))),"not found")
})

test_that("single-tile budget failure preserves a previous archive", {
  skip_on_cran();skip_if_not(.cluster_available())
  input <- .cluster_fixture(8000L);output <- tempfile(fileext=".pmtiles")
  on.exit(unlink(c(input,output)),add=TRUE)
  writeBin(charToRaw("previous"),output)
  withr::local_envvar(FREESTILER_CLUSTER_TILE_BUDGET_MB="1")
  expect_error(freestile_file(input,output,min_zoom=0,max_zoom=0,cluster_maxzoom=0,
    cluster_distance=1e-12,category="group_id",category_values=1:2,quiet=TRUE),
    "FREESTILER_CLUSTER_TILE_BUDGET_MB")
  expect_identical(readBin(output,"raw",n=8),charToRaw("previous"))
})
