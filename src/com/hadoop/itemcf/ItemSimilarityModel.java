package com.hadoop.itemcf;

/**
 * Created with IDEA
 * USER: Administrator
 * DATE: 2018/4/8
 *
 * @描述:
 */
public class ItemSimilarityModel implements Comparable<ItemSimilarityModel> {
    private Integer sourceItem;
    private Integer targetItem;
    private Double Similarity;

    @Override
    public int compareTo(ItemSimilarityModel o) {
        return this.getSimilarity().compareTo(o.getSimilarity());
//        Double thisSim=this.getSimilarity();
//        Double thatSim=o.getSimilarity();
//        if(thisSim-thatSim>0)
//            return 1;
//        else if(thisSim-thatSim<0)
//            return -1;
//        else
//            return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ItemSimilarityModel that = (ItemSimilarityModel) o;

        if (sourceItem != null ? !sourceItem.equals(that.sourceItem) : that.sourceItem != null) return false;
        if (targetItem != null ? !targetItem.equals(that.targetItem) : that.targetItem != null) return false;
        return Similarity != null ? Similarity.equals(that.Similarity) : that.Similarity == null;
    }

    @Override
    public int hashCode() {
        int result = sourceItem != null ? sourceItem.hashCode() : 0;
        result = 31 * result + (targetItem != null ? targetItem.hashCode() : 0);
        result = 31 * result + (Similarity != null ? Similarity.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ItemSimilarityModel{" +
                "sourceItem=" + sourceItem +
                ", targetItem=" + targetItem +
                ", Similarity=" + Similarity +
                '}';
    }

    public Integer getSourceItem() {
        return sourceItem;
    }

    public void setSourceItem(Integer sourceItem) {
        this.sourceItem = sourceItem;
    }

    public Integer getTargetItem() {
        return targetItem;
    }

    public void setTargetItem(Integer targetItem) {
        this.targetItem = targetItem;
    }

    public Double getSimilarity() {
        return Similarity;
    }

    public void setSimilarity(Double similarity) {
        Similarity = similarity;
    }
}
