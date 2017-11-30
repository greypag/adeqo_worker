require 'test_helper'

class ShenmasControllerTest < ActionController::TestCase
  setup do
    @shenma = shenmas(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:shenmas)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create shenma" do
    assert_difference('Shenma.count') do
      post :create, shenma: {  }
    end

    assert_redirected_to shenma_path(assigns(:shenma))
  end

  test "should show shenma" do
    get :show, id: @shenma
    assert_response :success
  end

  test "should get edit" do
    get :edit, id: @shenma
    assert_response :success
  end

  test "should update shenma" do
    patch :update, id: @shenma, shenma: {  }
    assert_redirected_to shenma_path(assigns(:shenma))
  end

  test "should destroy shenma" do
    assert_difference('Shenma.count', -1) do
      delete :destroy, id: @shenma
    end

    assert_redirected_to shenmas_path
  end
end
